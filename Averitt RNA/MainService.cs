using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.ServiceModel;
using System.ServiceProcess;
using Averitt_RNA.Apex;
using WindowsServiceUtility;

namespace Averitt_RNA
{
    partial class MainService : ServiceBase
    {

        #region Private Members

        private WSU _WSU;

        #endregion

        #region Thread Shared Members

        public static bool SessionRequired;
        public static bool RefreshCacheRequired;
        public static User User;
        public static SessionHeader SessionHeader;
        public static string QueryServiceUrl;
        public static string DefaultRoutingServiceUrl;
        public static List<Region> Regions;
        public static List<KeyValuePair<long, string>> NotificationRegionIdentifiers;
        public static List<long> BusinessUnitEntityKeys;
        public static Dictionary<long, UrlSet> RegionUrlSets;
        public static DateTime LastTruncateArchivesTime;
        public static Dictionary<long, Dictionary<string, int>> RegionGeocodeTimeoutCounts;

        #endregion

        #region Private Methods

        private void OnTimerElapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (SessionRequired || RefreshCacheRequired)
            {
                if (_WSU.Running)
                {
                    _WSU.StopProcessors(false);
                }
                if ((SessionRequired && CreateSession()) || (RefreshCacheRequired && RefreshCache()))
                {
                    _WSU.StartProcessors();
                }
            }
        }

        private bool CreateSession(bool starting = false)
        {
            bool success = false;
            try
            {
                
                Login();
                if (RefreshCache(starting))
                {
                    SessionRequired = false;
                    RefreshCacheRequired = false;
                    InitializeProcessors();
                    success = true;
                }
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                WSU.MainServiceLogger.Fatal("Create session failed with TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message);
                if (starting)
                {
                    throw;
                }
                else
                {
                    WSU.MainServiceLogger.Info("Transient error, suppressing exception.");
                }
            }
            catch (Exception ex)
            {
                WSU.MainServiceLogger.Fatal("Create session failed.", ex);
                if (starting)
                {
                    throw;
                }
                else
                {
                    WSU.MainServiceLogger.Info("Transient error, suppressing exception.");
                }
            }
            return success;
        }

        private void Login()
        {
            WSU.MainServiceLogger.Info("Login to Apex.");
            LoginServiceClient loginServiceClient = new LoginServiceClient();
            LoginResult loginResult = loginServiceClient.Login(
                Config.LOGIN_EMAIL,
                Config.LOGIN_PASSWORD,
                new CultureOptions(),
                new ClientApplicationInfo
                {
                    ClientApplicationIdentifier = new Guid(Config.CLIENT_APPLICATION_IDENTIFIER)
                });
            if (loginResult == null)
            {
                throw new Exception("Login to Apex failed with a null result.");
            }
            else
            {
                WSU.MainServiceLogger.Debug("Login completed successfully.");
                User = loginResult.User;
                SessionHeader = new SessionHeader { SessionGuid = loginResult.UserSession.Guid };
                QueryServiceUrl = loginResult.QueryServiceUrl;
                DefaultRoutingServiceUrl = loginResult.DefaultRoutingServiceUrl;
            }
        }

        private bool RefreshCache(bool starting = false)
        {
            bool success = false;
            try
            {
                BusinessUnitEntityKeys.Clear();
                RegionUrlSets.Clear();
                RegionGeocodeTimeoutCounts.Clear();
                QueryServiceClient queryServiceClient = new QueryServiceClient("BasicHttpBinding_IQueryService", QueryServiceUrl);
                RoutingServiceClient defaultRoutingServiceClient = new RoutingServiceClient("BasicHttpBinding_IRoutingService", DefaultRoutingServiceUrl);
                RetrieveRegions(queryServiceClient);
                foreach (Region region in Regions)
                {
                    RetrieveUrls(queryServiceClient, region);
                    RegionGeocodeTimeoutCounts.Add(region.EntityKey, new Dictionary<string, int>());
                }
                success = true;
            }
            catch (FaultException<TransferErrorCode> tec)
            {
                WSU.MainServiceLogger.Fatal("Refresh cache failed with TransferErrorCode: " + tec.Action + " | " + tec.Code.Name + " | " + tec.Detail.ErrorCode_Status + " | " + tec.Message);
                if (starting)
                {
                    throw;
                }
                else
                {
                    WSU.MainServiceLogger.Info("Transient error, suppressing exception.");
                }
            }
            catch (Exception ex)
            {
                WSU.MainServiceLogger.Fatal("Refresh cache failed.", ex);
                if (starting)
                {
                    throw;
                }
                else
                {
                    WSU.MainServiceLogger.Info("Transient error, suppressing exception.");
                }
            }
            return success;
        }

        private void RetrieveRegions(QueryServiceClient queryServiceClient)
        {
            WSU.MainServiceLogger.Info("Retrieve Regions.");
            RetrievalResults retrievalResults = queryServiceClient.RetrieveRegionsGrantingPermissions(
                SessionHeader,
                new RolePermission[] { },
                false);
            if (retrievalResults.Items == null)
            {
                throw new Exception("Retrieve Regions failed with a null result.");
            }
            else if (retrievalResults.Items.Length == 0)
            {
                throw new Exception("No Regions exist.");
            }
            else
            {
                WSU.MainServiceLogger.Debug("Retrieve Regions completed successfully.");
                Regions = retrievalResults.Items.Cast<Region>().OrderBy(region => region.Identifier).ToList();
                BusinessUnitEntityKeys = Regions.Select(region => region.BusinessUnitEntityKey).Distinct().ToList();
                WSU.MainServiceLogger.Info(Config.BUSINESS_UNIT_ENTITY_KEY_FILTER.Length == 0 ? "Business Unit Entity Key Filter disabled. Use all Business Units." : ("Only use Business Units: " + string.Join(", ", Config.BUSINESS_UNIT_ENTITY_KEY_FILTER)));
                if (Config.BUSINESS_UNIT_ENTITY_KEY_FILTER.Length > 0)
                {
                    List<long> invalidFilters = Config.BUSINESS_UNIT_ENTITY_KEY_FILTER.ToList();
                    invalidFilters.RemoveAll(filter => BusinessUnitEntityKeys.Contains(filter));
                    if (invalidFilters.Count > 0)
                    {
                        WSU.MainServiceLogger.Warn("Business Unit Entity Key Filter contains invalid entries: " + string.Join(", ", invalidFilters));
                    }
                    WSU.MainServiceLogger.Debug("Applying Business Unit Entity Key Filter.");
                    BusinessUnitEntityKeys.RemoveAll(entityKey => !Config.BUSINESS_UNIT_ENTITY_KEY_FILTER.Contains(entityKey));
                    if (BusinessUnitEntityKeys.Count == 0)
                    {
                        throw new Exception("No valid Business Units specified.");
                    }
                    Regions.RemoveAll(region => !Config.BUSINESS_UNIT_ENTITY_KEY_FILTER.Contains(region.BusinessUnitEntityKey));
                }
                NotificationRegionIdentifiers = Regions.Select(region => new KeyValuePair<long, string>(region.EntityKey, region.Identifier)).ToList();
                string[] regionIdentifiers = Regions.Select(region => region.Identifier).ToArray();
                WSU.MainServiceLogger.Debug("Regions list contains: " + string.Join(", ", regionIdentifiers));
                WSU.MainServiceLogger.Info(Config.REGION_FILTER.Length == 0 ? "Region Filter disabled. Use all Regions." : (Config.REGION_FILTER.Length == 1 && Config.REGION_FILTER[0] == "NONE" ? "All Regions disabled." : ("Only use Regions: " + string.Join(", ", Config.REGION_FILTER))));
                if (Config.REGION_FILTER.Length == 1 && Config.REGION_FILTER[0] == "NONE")
                {
                    Regions.Clear();
                }
                else if (Config.REGION_FILTER.Length > 0)
                {
                    List<string> invalidFilters = Config.REGION_FILTER.ToList();
                    invalidFilters.RemoveAll(filter => regionIdentifiers.Contains(filter));
                    if (invalidFilters.Count > 0)
                    {
                        WSU.MainServiceLogger.Warn("Region Filter contains invalid entries: " + string.Join(", ", invalidFilters));
                    }
                    WSU.MainServiceLogger.Debug("Applying Region Filter.");
                    Regions.RemoveAll(region => !Config.REGION_FILTER.Contains(region.Identifier));
                    if (Regions.Count == 0)
                    {
                        throw new Exception("No valid Regions specified.");
                    }
                }
                if (Config.ENABLE_NOTIFICATIONS_PROCESS)
                {
                    if (Config.NOTIFICATION_REGION_FILTER.Length == 0)
                    {
                        WSU.MainServiceLogger.Info("Notification Region Filter disabled. Use all Regions.");
                    }
                    else
                    {
                        WSU.MainServiceLogger.Info("For Notifications, only use Regions: " + string.Join(", ", Config.NOTIFICATION_REGION_FILTER));
                        List<string> invalidFilters = Config.NOTIFICATION_REGION_FILTER.ToList();
                        invalidFilters.RemoveAll(filter => regionIdentifiers.Contains(filter));
                        if (invalidFilters.Count > 0)
                        {
                            WSU.MainServiceLogger.Warn("Notification Region Filter contains invalid entries: " + string.Join(", ", invalidFilters));
                        }
                        WSU.MainServiceLogger.Debug("Applying Notification Region Filter.");
                        NotificationRegionIdentifiers.RemoveAll(pair => !Config.NOTIFICATION_REGION_FILTER.Contains(pair.Value));
                        if (NotificationRegionIdentifiers.Count == 0)
                        {
                            throw new Exception("No valid Notification Regions specified.");
                        }
                    }
                }
            }
        }

        private void RetrieveUrls(QueryServiceClient queryServiceClient, Region region)
        {
            WSU.MainServiceLogger.Info("Retrieve Urls for Region: " + ApexConsumer.ToString(region));
            UrlSet urlSet = queryServiceClient.RetrieveUrlsForContext(
                SessionHeader,
                new SingleRegionContext
                {
                    BusinessUnitEntityKey = region.BusinessUnitEntityKey,
                    RegionEntityKey = region.EntityKey
                });
            if (urlSet == null)
            {
                throw new Exception("Retrieve Urls failed with a null result for Region: " + ApexConsumer.ToString(region));
            }
            else
            {
                WSU.MainServiceLogger.Debug("Retrieve Urls completed successfully for Region: " + ApexConsumer.ToString(region));
                RegionUrlSets.Add(region.EntityKey, urlSet);
            }
        }

        private void InitializeProcessors()
        {
            _WSU.Processors.Clear();
            foreach (Region region in Regions)
            {
                _WSU.Processors.Add("RegionProcessor_" + region.Identifier, new RegionProcessor(region));
            }
            if (Config.ENABLE_ARCHIVE_PROCESS)
            {
                _WSU.Processors.Add("ArchivesProcessor", new ArchivesProcessor());
            }
            if (Config.ENABLE_MAINTENANCE_PROCESS)
            {
                foreach (long businessUnitEntityKey in BusinessUnitEntityKeys)
                {
                    _WSU.Processors.Add("MaintenanceProcessor_" + businessUnitEntityKey, new MaintenanceProcessor(businessUnitEntityKey));
                }
            }
            if (Config.ENABLE_NOTIFICATIONS_PROCESS)
            {
                foreach (long businessUnitEntityKey in BusinessUnitEntityKeys)
                {
                    _WSU.Processors.Add("NotificationsProcessor_" + businessUnitEntityKey, new NotificationsProcessor(businessUnitEntityKey));
                }
            }
        }

        #endregion

        #region Protected Methods

        protected override void OnStart(string[] args)
        {
            BusinessUnitEntityKeys = new List<long>();
            RegionUrlSets = new Dictionary<long, UrlSet>();
            DateTime now = TimeZoneInfo.ConvertTimeBySystemTimeZoneId(DateTime.UtcNow, WSU.ServerTimeZone);
            LastTruncateArchivesTime = Config.TRUNCATE_ARCHIVES_TIMES.Length > 0 ? now : DateTime.MaxValue;
            RegionGeocodeTimeoutCounts = new Dictionary<long, Dictionary<string, int>>();
            SessionRequired = true;
            if (!CreateSession(true))
            {
                throw new Exception("Failed to create session. Aborting.");
            }
            _WSU.StartProcessors();
        }

        protected override void OnStop()
        {
            RequestAdditionalTime(120000);
            _WSU.StopProcessors();
        }

        #endregion

        #region Public Methods

        public MainService()
        {
            InitializeComponent();
            _WSU = new WSU(Config.LOG_FILE_PATH, Config.RUN_INTERVAL, Config.MAXIMUM_THREADS, Config.SLEEP_DURATION);
            _WSU.Timer.Elapsed += new System.Timers.ElapsedEventHandler(OnTimerElapsed);
        }

        [Conditional("DEBUG")]
        public void OnDebug()
        {
            OnStart(null);
        }

        #endregion

    }
}
