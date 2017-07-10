using System;
using System.Configuration;
using System.Linq;

namespace Averitt_RNA
{
    class Config
    {
        public static readonly int ARCHIVE_DAYS = Convert.ToInt32(ConfigurationManager.AppSettings["ArchiveDays"]);
        public static readonly long[] BUSINESS_UNIT_ENTITY_KEY_FILTER = ConfigurationManager.AppSettings["BusinessUnitEntityKeyFilter"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(entityKey => Convert.ToInt64(entityKey)).ToArray();
        public static readonly string CLIENT_APPLICATION_IDENTIFIER = ConfigurationManager.AppSettings["ClientApplicationIdentifier"];
        public static readonly bool ENABLE_ARCHIVE_PROCESS = Convert.ToBoolean(ConfigurationManager.AppSettings["EnableArchiveProcess"]);
        public static readonly bool ENABLE_MAINTENANCE_PROCESS = Convert.ToBoolean(ConfigurationManager.AppSettings["EnableMaintenanceProcess"]);
        public static readonly bool ENABLE_NOTIFICATIONS_PROCESS = Convert.ToBoolean(ConfigurationManager.AppSettings["EnableNotificationsProcess"]);
        public static readonly int HELD_THREAD_SLEEP_DURATION = Convert.ToInt32(ConfigurationManager.AppSettings["HeldThreadSleepDuration"]);
        public static readonly string LOG_FILE_PATH = ConfigurationManager.AppSettings["LogFilePath"];
        public static readonly string LOGIN_EMAIL = ConfigurationManager.AppSettings["LoginEmail"];
        public static readonly string LOGIN_PASSWORD = ConfigurationManager.AppSettings["LoginPassword"];
        public static readonly long MAXIMUM_THREADS = Convert.ToInt64(ConfigurationManager.AppSettings["MaximumThreads"]);
        public static readonly string[] NOTIFICATION_REGION_FILTER = ConfigurationManager.AppSettings["NotificationRegionFilter"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        public static readonly string[] REGION_FILTER = ConfigurationManager.AppSettings["RegionFilter"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
        public static readonly double RUN_INTERVAL = Convert.ToDouble(ConfigurationManager.AppSettings["RunInterval"]);
        public static readonly int SLEEP_DURATION = Convert.ToInt32(ConfigurationManager.AppSettings["SleepDuration"]);
        public static readonly TimeSpan[] TRUNCATE_ARCHIVES_TIMES = ConfigurationManager.AppSettings["TruncateArchivesTimes"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(time => TimeSpan.Parse(time)).ToArray();
        public static readonly string DummyOrderCSVFile = ConfigurationManager.AppSettings["DummyOrderCSVFile"];
        public static readonly int DictServiceTimeRefresh = Convert.ToInt32(ConfigurationManager.AppSettings["DictServiceTimeRefresh"]);
    }
}
