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

(defn list-db [conn]
  (mapv :pg_database/datname
        (pg/execute! conn ["SELECT datname FROM pg_database"])))

(defn exists-db? [conn db-name]
  (contains? (set (list-db conn)) db-name))

(defn ensure-db-exists! [conn db-name]
  (when-not (exists-db? conn db-name)
    (throw (ex-info "Db not exists" {:db-name db-name}))))

(defn ensure-db-not-exists! [conn db-name]
  (when (exists-db? conn db-name)
    (throw (ex-info "Db exists" {:db-name db-name}))))

(defn create-db-with-template [conn template-name db-name]
  (ensure-db-exists! conn template-name)
  (ensure-db-not-exists! conn db-name)
  (pg/execute! conn [(format "CREATE DATABASE \"%s\" WITH TEMPLATE \"%s\""
                             db-name
                             template-name)]))

(defn rename-db [conn db-name db-new-name]
  (ensure-db-exists! conn db-name)
  (ensure-db-not-exists! conn db-new-name)
  (pg/execute! conn [(format "ALTER DATABASE \"%s\" RENAME TO \"%s\""
                             db-name
                             db-new-name)]))

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
  {:spec {:port     {:coerce  :long
                     :alias   :p
                     :desc    "PostgreSQL port"
                     :default 5432
                     :valid   pos?}
          :password {:desc    "PostgreSQL password"
                     :alias   :P
                     :default "postgres"}
          :user     {:desc    "PostgreSQL user"
                     :alias   :U
                     :default "postgres"}
          :host     {:alias   :H
                     :desc    "PostgreSQL host"
                     :default "localhost"}
          :help     {:coerce :boolean
                     :alias  :h
                     :desc   "Help message"}
          :list     {:coerce :boolean
                     :alias  :l
                     :desc   "List db and backups"}
          :backup   {:alias :b
                     :desc  "Create db backup"}
          :restore  {:alias :r
                     :desc  "Restore db from backup"}}})

(defn run-help []
  (println "Usage:" *file* "[options]")
  (println "A utility for creating and managing PostgreSQL database backups.")
  (println)
  (println "Options:")
  (println (cli/format-opts cli-spec))
  (println)
  (println "Note: This utility is for development purposes only."
           "Backups are stored as databases.")
  (println "It does not protect against data corruption but can be used"
           "to reset the development environment."))

(defn run-list [conn]
  (let [l (list-db conn)
        by-backup (group-by backup-of l)]
    (doseq [n (get by-backup nil)]
      (println n)
      (doseq [bn (get by-backup n)]
        (println "\t" bn)))))

(defn run-backup [conn db-name]
  (let [bn (backup-db-name db-name)]
    (println "DB:\t" db-name "\nBackup:\t" bn)
    (create-db-with-template conn db-name bn)))

(defn run-restore [conn db-backup-name]
  (if-let [db-name (backup-of db-backup-name)]
    (let [new-backup (backup-db-name db-name)]
      (rename-db conn db-name new-backup)
      (try
        (create-db-with-template conn db-backup-name db-name)
        (catch Exception e
          (rename-db conn new-backup db-name)
          (throw e))))
    (println "Not backup: " db-backup-name)))

(defn main [args]
  (let [opts (cli/parse-opts args cli-spec)
        conn (make-conn opts)]
    (cond
      (opts :help) (run-help)
      (opts :list) (run-list conn)
      (opts :backup) (run-backup conn (opts :backup))
      (opts :restore) (run-restore conn (opts :restore))
      :else (run-help))))

(main *command-line-args*)
