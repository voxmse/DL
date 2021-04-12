# DL

The idea is create a simple "Data Lake" application to:
 * collect data from different sources
          * minimal required configuration
          * fast with minimal resource consumption
          * file storage
          * schema-per-row
          * database to store schema data
 * extract and synchronize collected data with external destinations
          * different export/synch formats
          * extracted data schema formats
          * database to store system data
          * locking and change time tracking system for consistent operations
          * simple command line interface
