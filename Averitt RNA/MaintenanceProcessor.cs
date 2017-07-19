using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Averitt_RNA.Apex;
using Averitt_RNA.DBAccess;
using WindowsServiceUtility;
using Averitt_RNA.DBAccess.Records;

namespace Averitt_RNA
{
    class MaintenanceProcessor : Processor
    {

        private long _BusinessUnitEntityKey;
        private ApexConsumer _ApexConsumer;
        private IntegrationDBAccessor _IntegrationDBAccessor;
        private ApexConsumer.ErrorLevel errorLevel;


        public MaintenanceProcessor(long businessUnitEntityKey)
            : base(MethodBase.GetCurrentMethod().DeclaringType, businessUnitEntityKey.ToString())
        {
            _BusinessUnitEntityKey = businessUnitEntityKey;
            _ApexConsumer = new ApexConsumer(businessUnitEntityKey, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
        }

        public override void Process()
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            if (!MainService.SessionRequired)
            {

                //TODO
                //List<StagedSKURecord> stagedSKURecords = RetrieveStagedSKURecords();
                //if (stagedSKURecords != null)
                //{
                //    RetrieveExistingSKUs(stagedSKURecords);
                //    SaveSKUs(stagedSKURecords);
                //}

                try
                {
                    
                    DeleteExpiredOrders(out errorCaught, out errorMessage);
                    DeleteExpiredServiceLocations(out errorCaught, out errorMessage);
                    DeleteExpiredRoutes(out errorCaught, out errorMessage);

                    if (errorCaught)
                    {
                        Logger.Error("Error Performing Maintenance on Expired Records from Staging Tables" + errorMessage);

                    }
                }
                catch (Exception ex)
                {
                    Logger.Error(" Error Performing Maintenance on Expired Records from Staging Tables" + ex.Message);
                }

            }
            else
            {
                Logger.Info("Waiting for Session.");
            }

        }

        public void DeleteExpiredOrders(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
                List<StagedOrderRecord> errorOrders = _IntegrationDBAccessor.SelectAllStagedOrdersStatus("ERROR", out errorMessage, out errorCaught);
                
                if (errorCaught)
                {
                    Logger.Error("Error Retrieving Staged Order Records from STAGED_ORDERS" + errorMessage);

                }
                else
                {
                    foreach(StagedOrderRecord record in errorOrders)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged,out stagedDate);
                        if (stagedDate.Day < (DateTime.Now.Day - Config.ARCHIVE_DAYS))
                        {
                            _IntegrationDBAccessor.DeleteExpiredOrder(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status,record.Staged, out errorMessage, out errorCaught);

                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Staged Order Records from STAGED_ORDERS" + errorMessage);

                            }
                        }
                    }
                }
                List<StagedOrderRecord> completeOrders = _IntegrationDBAccessor.SelectAllStagedOrdersStatus("COMPLETE", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.Error("Error Retrieving Staged Order Records from STAGED_ORDERS" + errorMessage);

                }
                {
                    foreach (StagedOrderRecord record in completeOrders)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (stagedDate.Day < (DateTime.Now.Day - Config.ARCHIVE_DAYS))
                        {
                            _IntegrationDBAccessor.DeleteExpiredOrder(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status, record.Staged , out errorMessage, out errorCaught);

                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Staged Order Records from STAGED_ORDERS" + errorMessage);

                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.Error("Error Deleting Staged Order Records from STAGED_ORDERS" + ex.Message);
            }




        }

        public void DeleteExpiredServiceLocations(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
                List<StagedServiceLocationRecord> errorSL = _IntegrationDBAccessor.SelectAllStagedServiceLocationsStatus("ERROR", out errorMessage, out errorCaught);

                if (errorCaught)
                {
                    Logger.Error("Error Retrieving Service Location Records from STAGED_SERVICE_LOCATIONS" + errorMessage);

                }
                else
                {
                    foreach (StagedServiceLocationRecord record in errorSL)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (stagedDate.Day < (DateTime.Now.Day - Config.ARCHIVE_DAYS))
                        {
                            _IntegrationDBAccessor.DeleteExpiredStagedServiceLocation(record.RegionIdentifier, record.ServiceLocationIdentifier,
                                record.Staged, out errorMessage, out errorCaught);

                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Service Location Records from STAGED_SERVICE_LOCATIONS" + errorMessage);

                            }
                        }
                    }
                }
                List<StagedServiceLocationRecord> completeSL = _IntegrationDBAccessor.SelectAllStagedServiceLocationsStatus("COMPLETE", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.Error("Error Retrieving Service Location Records from STAGED_SERVICE_LOCATIONS" + errorMessage);

                }
                {
                    foreach (StagedServiceLocationRecord record in completeSL)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (stagedDate.Day < (DateTime.Now.Day - Config.ARCHIVE_DAYS))
                        {
                            _IntegrationDBAccessor.DeleteExpiredStagedServiceLocation(record.RegionIdentifier, record.ServiceLocationIdentifier,
                                record.Staged, out errorMessage, out errorCaught);

                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Service Location Records from STAGED_SERVICE_LOCATIONS" + errorMessage);

                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.Error("Error Deleting Service Location Records from STAGED_SERVICE_LOCATIONS" + ex.Message);
            }




        }

        public void DeleteExpiredRoutes(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
              List<StagedRouteRecord> errorRoutes = _IntegrationDBAccessor.SelectStagedRoutesStatus("ERROR", out errorMessage, out errorCaught);

                if (errorCaught)
                {
                    Logger.Error("Error Retrieving Route Records from STAGED_ROUTES" + errorMessage);

                }
                else
                {
                    foreach (StagedRouteRecord record in errorRoutes)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (stagedDate.Day < (DateTime.Now.Day - Config.ARCHIVE_DAYS))
                        {
                            _IntegrationDBAccessor.DeleteExpiredStagedRoute(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status, record.Staged, out errorMessage, out errorCaught);

                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Route Records from STAGED_ROUTES" + errorMessage);

                            }
                        }
                    }
                }
                List<StagedRouteRecord> completeRoutes = _IntegrationDBAccessor.SelectStagedRoutesStatus("COMPLETE", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.Error("Error Retrieving  Route Records from STAGED_ROUTES" + errorMessage);

                }
                {
                    foreach (StagedRouteRecord record in completeRoutes)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (stagedDate.Day < (DateTime.Now.Day - Config.ARCHIVE_DAYS))
                        {
                            _IntegrationDBAccessor.DeleteExpiredStagedRoute(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status, record.Staged, out errorMessage, out errorCaught);

                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting  Route Records from STAGED_ROUTES" + errorMessage);

                            }
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.Error("Error Deleting  Route Records from STAGED_ROUTES" + ex.Message);
            }




        }
    }
}
