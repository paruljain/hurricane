# Hurricane
High performance file backup and restore using AWS s3 as target

# Vision:

1. Backup filesystems containing millions of files totalling terrabytes
2. Full and incremental backups while minimizing requests (cost) to s3
3. Full and partial restores upto a single file
4. No limit for number of files, total size, or single file size
5. Support for on-prem s3 compatible targets such as Minio, IBM COS, etc.
6. Hurricane will be optional for restore. This will make sure backup is available even if Hurricane is not available for any reason. Each file will map to one object and the object will have complete file metadata to faithfully reconstruct the file
7. High performance even for small files and average (30 ms) network latency to s3 target. Copying a large number of small files over high latency network has always been a challenge
8. Optional compression and encryption
9. User does not need to configure Hurricane. Hurricane will automatically set read concurrency, write concurrency and buffer
