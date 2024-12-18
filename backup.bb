#!/usr/bin/env bb

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                              deps and imports                              ;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(require '[babashka.pods :as pods])
(pods/load-pod 'org.babashka/postgresql "0.1.2")
(require '[pod.babashka.postgresql :as pg])

(require '[babashka.cli :as cli])

(import '[java.time Instant]
        '[java.time ZoneId]
        '[java.time.format DateTimeFormatter])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                   private                                  ;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-conn [opts]
  (-> (select-keys opts [:host :user :password :port])
      (assoc :dbtype "postgresql")))

(defn list-db [db]
  (mapv :pg_database/datname
        (pg/execute! db ["SELECT datname FROM pg_database"])))

(defn create-db-with-template [db template-name db-name]
  (pg/execute! db [(format "CREATE DATABASE \"%s\" WITH TEMPLATE \"%s\""
                           db-name
                           template-name)]))

(defn rename-db [db db-name db-new-name]
  (pg/execute! db [(format "ALTER DATABASE \"%s\" RENAME TO \"%s\""
                           db-name
                           db-new-name)]))

(defn exists-db? [db db-name]
  (contains? (set (list-db db)) db-name))

(def backup-db-re #"__backup-\d{4}-\d{2}-\d{2}-\d{10}-(.+)")

(defn backup-db-name [db-name]
  (let [format  (DateTimeFormatter/ofPattern "yyyy-MM-dd")
        now     (Instant/now)]
    (str "__backup-"
         (.format format
                  (.atZone now (ZoneId/of "UTC")))
         "-"
         (.getEpochSecond now)
         "-"
         db-name)))

(defn backup-of [db-name]
  (second (re-find backup-db-re db-name)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                                 public api                                 ;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def cli-spec
  {:spec {:port {:coerce :long
                 :alias :p
                 :desc "PostgreSQL port"
                 :default 5432
                 :valid pos?}
          :password {:desc "PostgreSQL password"
                     :default "postgres"}
          :user {:desc "PostgreSQL user"
                 :default "postgres"}
          :host {:alias :h
                 :desc "PostgreSQL host"
                 :default "localhost"}
          :help {:coerce :boolean
                 :desc "Help message"}
          :list {:coerce :boolean
                 :alias :l
                 :desc "list db"}
          :backup {:alias :b
                   :desc "create db backup"}
          :restore {:alias :r
                    :desc "Restore db from backup"}}})

(defn run-help []
  (println (cli/format-opts cli-spec)))

(defn run-list [db]
  (let [l (list-db db)
        by-backup (group-by backup-of l)]
    (doseq [n (get by-backup nil)]
      (println n)
      (doseq [bn (get by-backup n)]
        (println "\t" bn)))))

(defn run-backup [db db-name]
  (let [bn (backup-db-name db-name)]
    (println "DB:\t" db-name "\nBackup:\t" bn)
    (create-db-with-template db db-name bn)))

(defn run-restore [db db-backup-name]
  (if-let [db-name (backup-of db-backup-name)]
    (let [new-backup (backup-db-name db-name)]
      (rename-db db db-name new-backup)
      (try
        (create-db-with-template db db-backup-name db-name)
        (catch Exception e
          (rename-db db new-backup db-name)
          (throw e))))
    (println "Not backup: " db-backup-name)))

(defn main [args]
  (let [opts (cli/parse-opts args cli-spec)
        db (make-conn opts)]
    (cond
      (opts :help) (run-help)
      (opts :list) (run-list db)
      (opts :backup) (run-backup db (opts :backup))
      (opts :restore) (run-restore db (opts :restore))
      :else (run-help))))

(main *command-line-args*)
