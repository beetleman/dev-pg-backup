# Backup tool

```
Usage: pg-backup.bb [options]
A utility for creating and managing PostgreSQL database backups.
Options:
  -p, --port     5432      PostgreSQL port
      --password postgres  PostgreSQL password
      --user     postgres  PostgreSQL user
  -h, --host     localhost PostgreSQL host
      --help               Help message
  -l, --list               List db and backups
  -b, --backup             Create db backup
  -r, --restore            Restore db from backup
Note: This utility is for development purposes only. Backups are stored as databases.
It does not protect against data corruption but can be used to reset the development environment.
```
