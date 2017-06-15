using System;
using System.IO;
using System.Reflection;
using Averitt_RNA.DBAccess;
using WindowsServiceUtility;

namespace Averitt_RNA
{
    class ArchivesProcessor : Processor
    {

        private IntegrationDBAccessor _IntegrationDBAccessor;

        public ArchivesProcessor()
            : base(MethodBase.GetCurrentMethod().DeclaringType)
        {
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
        }

        public override void Process()
        {

            if (WSU.TriggerTimeElapsed(MainService.LastTruncateArchivesTime, Config.TRUNCATE_ARCHIVES_TIMES) > 0)
            {
                DateTime now = DateTime.UtcNow;
                DateTime boundary = now.AddDays(-1 * Config.ARCHIVE_DAYS);
                Logger.Debug("Truncate staging tables.");
                //TODO
                //if (_IntegrationDBAccessor.Delete(boundary))
                //{
                string[] logFiles = Directory.GetFiles(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Config.LOG_FILE_PATH), "*.txt", SearchOption.TopDirectoryOnly);
                foreach (string logFile in logFiles)
                {
                    FileInfo fileInfo = new FileInfo(logFile);
                    if (fileInfo.CreationTimeUtc < boundary)
                    {
                        fileInfo.Delete();
                    }
                }
                Logger.Debug("Staging tables truncated successfully.");
                MainService.LastTruncateArchivesTime = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(now, WSU.ServerTimeZone);
                //}
            }

        }

    }
}
