using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Amazon.S3.Transfer;
using Amazon.S3;
using System.IO;

namespace CPREnvoy
{
    public class S3UploadException: Exception
    {
        public S3UploadException() : base() { }
        public S3UploadException(string message) : base(message) { }
    }

    public class S3Uploader
    {

        public static void upload(string filePath, string keyName, string existingBucketName)
        {
            try
            {
                TransferUtility fileTransferUtility = new
                    TransferUtility(new AmazonS3Client(Amazon.RegionEndpoint.USEast1));


                TransferUtilityUploadRequest fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName = existingBucketName,
                    FilePath = filePath,
                    StorageClass = S3StorageClass.ReducedRedundancy,
                    //PartSize = 6291456, // 6 MB.
                    Key = keyName,
                    CannedACL = S3CannedACL.Private
                };

                fileTransferUtility.Upload(fileTransferUtilityRequest);

                Console.WriteLine("Upload completed");
            }
            catch (Exception s3Exception)
            {
                throw new S3UploadException(s3Exception.Message);
            }
        }

        public static string GenerateDownloadLink(string tableName, string customerCode, string customerCodeEnv, string dataFormat="csv")
        {
            return String.Format("https://s3.amazonaws.com/ecpr/{0}{1}/{2}.{3}.gz", customerCode, customerCodeEnv, tableName.ToLower(), dataFormat);
        }
    }
}
