# Introduction

This is small program for zipping a folder in S3 bucket then save result file S3 bucket. This program don't download all files to local so we can use it to zip large folder without care about disk full.

# Usage

```
s3zipfolder <bucket> <prefix> <region>
```

OR 

```
s3zipfolder <bucket> <prefix> <region> <aws key> <aws secret>
```

# Example

```
./s3zipfolder bucket-data-test 20200414/ ap-northeast-1
```

```
./s3zipfolder bucket-data-test 20200414/ ap-northeast-1 AZZDDDAAAAXXXXXEEE QRsdzhheeza97oadfrraddccaedf3
```