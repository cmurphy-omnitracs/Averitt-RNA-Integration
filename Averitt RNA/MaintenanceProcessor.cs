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
                    Logger.Error("Start Cleaning Expired and Competed Orders");
                    DeleteExpiredOrdersSQL(out errorCaught, out errorMessage);
                    Logger.Error("Cleaning Successful");
                    Logger.Error("Start Cleaning Expired and Completed Service Locationss");
                    DeleteExpiredSLSQL(out errorCaught, out errorMessage);
                    Logger.Error("Cleaning Successful");
                    Logger.Error("Start Cleaning Expired and Completed Service Routes");
                    DeleteExpiredRoutesSQL(out errorCaught, out errorMessage);
                    Logger.Error("Cleaning Successful");

                    if (errorCaught)
                    {
                        Logger.Error("Error Performing Maintenance on Expired Records from Staging Tables" + errorMessage);

                    }
                }
                catch (Exception ex)
                {
                    Logger.Error("Error Performing Maintenance on Expired Records from Staging Tables" + ex.Message);
                }

            }
            else
            {
                Logger.Info("Waiting for Session.");
            }

        }

        public void DeleteExpiredOrdersSQL(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
                Logger.DebugFormat("Started Deleting Expired Orders with a status of Error or Complete");
                _IntegrationDBAccessor.DeleteExpiredOrderSQL(out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.Error("Error Deleting Expired Orders" + errorMessage);

                }
                else
                {
                    Logger.Debug("Deleting Expired Orders Successful");
                }
            }

            catch (Exception ex)
            {
                Logger.Error("Error Deleting Staged Order Records from STAGED_ORDERS" + ex.Message);
            }
            
        }

        public void DeleteExpiredRoutesSQL(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
                Logger.DebugFormat("Started Deleting Expired Routes with a status of Error or Complete");
                _IntegrationDBAccessor.DeleteExpiredRoutesSQL(out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.Error("Error Deleting Expired Routes" + errorMessage);

                }
                else
                {
                    Logger.Debug("Deleting Expired Routes Successful");
                }
            }

            catch (Exception ex)
            {
                Logger.Error("Error Deleting Staged Routes Records from STAGED_ROUTES" + ex.Message);
            }

        }

        public void DeleteExpiredSLSQL(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
                Logger.DebugFormat("Started Deleting Expired Service Locations with a status of Error or Complete");
                _IntegrationDBAccessor.DeleteExpiredSLSQL(out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.Error("Error Deleting Expired Service Locations" + errorMessage);

                }
                else
                {
                    Logger.Debug("Deleting Expired Service Locations Successful");
                }
            }

            catch (Exception ex)
            {
                Logger.Error("Error Deleting Staged Serivice Locations Records from STAGED_SERVICE_LOCATIONS" + ex.Message);
            }

        }

        public void DeleteExpiredOrders(out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            try
            {
                List<StagedOrderRecord> errorOrders = _IntegrationDBAccessor.SelectAllStagedOrdersStatus("ERROR", out errorMessage, out errorCaught);
                
                Logger.DebugFormat("Retrieved {0} Error Orders from Database", errorOrders.Count);
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
                        if (Config.ARCHIVE_DAYS <= (DateTime.Now.Day - stagedDate.Day))
                        {
                            Logger.DebugFormat("Order {0} Staged date is less than {1} day, Record removed from database", record.OrderIdentifier, Config.ARCHIVE_DAYS);
                            _IntegrationDBAccessor.DeleteExpiredOrder(record.RegionIdentifier, record.OrderIdentifier, record.Status,record.Staged, out errorMessage, out errorCaught);
                            Logger.DebugFormat("Removing {0} Successful", record.OrderIdentifier);
                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Staged Order Records from STAGED_ORDERS" + errorMessage);

                            }
                        }
                    }
                }
                List<StagedOrderRecord> completeOrders = _IntegrationDBAccessor.SelectAllStagedOrdersStatus("COMPLETE", out errorMessage, out errorCaught);
                Logger.DebugFormat("Retrieved {0} Complete Orders from Database", completeOrders.Count);
                if (errorCaught)
                {
                    Logger.Error("Error Retrieving Staged Order Records from STAGED_ORDERS" + errorMessage);

                }
                {
                    foreach (StagedOrderRecord record in completeOrders)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (Config.ARCHIVE_DAYS <= (DateTime.Now.Day - stagedDate.Day))
                        {
                            Logger.DebugFormat("Order {0} Staged date is less than {1} day, Record removed from database", record.OrderIdentifier, Config.ARCHIVE_DAYS);
                            _IntegrationDBAccessor.DeleteExpiredOrder(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status, record.Staged , out errorMessage, out errorCaught);
                            Logger.DebugFormat("Removing Order {0} Successful", record.OrderIdentifier);
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
                Logger.DebugFormat("Retrieved {0} Error Service Locations from Database", errorSL.Count);
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
                        if (Config.ARCHIVE_DAYS <= (DateTime.Now.Day - stagedDate.Day))
                        {
                            Logger.DebugFormat("Service Location {0} Staged date is less than {1} day, Record removed from database", record.ServiceLocationIdentifier, Config.ARCHIVE_DAYS);
                            _IntegrationDBAccessor.DeleteExpiredStagedServiceLocation(record.RegionIdentifier, record.ServiceLocationIdentifier,
                                record.Staged, out errorMessage, out errorCaught);
                            Logger.DebugFormat("Removing Service Location {0} Successful", record.ServiceLocationIdentifier);
                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Service Location Records from STAGED_SERVICE_LOCATIONS" + errorMessage);

                            }
                        }
                    }
                }
                List<StagedServiceLocationRecord> completeSL = _IntegrationDBAccessor.SelectAllStagedServiceLocationsStatus("COMPLETE", out errorMessage, out errorCaught);
                Logger.DebugFormat("Retrieved {0} Completed Service Locations from Database", errorSL.Count);
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
                            Logger.DebugFormat("Service Location {0} Staged date is less than {1} day, Record removed from database", record.ServiceLocationIdentifier, Config.ARCHIVE_DAYS);
                            _IntegrationDBAccessor.DeleteExpiredStagedServiceLocation(record.RegionIdentifier, record.ServiceLocationIdentifier,
                                record.Staged, out errorMessage, out errorCaught);
                            Logger.DebugFormat("Removing Service Location {0} Successful", record.ServiceLocationIdentifier);
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
                Logger.DebugFormat("Retrieved {0} Error Routes and Orders from Database", errorRoutes.Count);
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
                        if (Config.ARCHIVE_DAYS <= (DateTime.Now.Day - stagedDate.Day))
                        {
                            
                            if(record.RouteIdentifier != null || record.RouteIdentifier.Length > 0)
                            {
                                Logger.DebugFormat("Route {0} with  Order {1} Staged date is less than {2} day, Record removed from database", record.RouteIdentifier, record.OrderIdentifier, Config.ARCHIVE_DAYS);
                                _IntegrationDBAccessor.DeleteExpiredStagedRouteAndOrder(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status, record.Staged, record.RouteIdentifier, out errorMessage, out errorCaught);
                                Logger.DebugFormat("Removal of Route {0} ,  Order {1} completed successfully", record.RouteIdentifier, record.OrderIdentifier);
                            } else
                            {
                                Logger.DebugFormat("Order {1} Staged date is less than {2} day, Record removed from database", record.OrderIdentifier, Config.ARCHIVE_DAYS);
                                _IntegrationDBAccessor.DeleteExpiredStagedRouteOrder(record.RegionIdentifier, record.OrderIdentifier,
                               record.Status, record.Staged, out errorMessage, out errorCaught);
                                Logger.DebugFormat("Removal of Order {1} From Staged Route Table completed successfully", record.RouteIdentifier, record.OrderIdentifier);
                            }
                           
                            if (errorCaught)
                            {
                                Logger.Error("Error Deleting Route Records from STAGED_ROUTES" + errorMessage);

                            }
                        }
                    }
                }
                List<StagedRouteRecord> completeRoutes = _IntegrationDBAccessor.SelectStagedRoutesStatus("COMPLETE", out errorMessage, out errorCaught);
                Logger.DebugFormat("Retrieved {0} Comlpeted Routes and Orders from Database", errorRoutes.Count);
                if (errorCaught)
                {
                    Logger.Error("Error Retrieving  Route Records from STAGED_ROUTES" + errorMessage);

                }
                {
                    foreach (StagedRouteRecord record in completeRoutes)
                    {
                        DateTime stagedDate;
                        bool parseDatetime = DateTime.TryParse(record.Staged, out stagedDate);
                        if (Config.ARCHIVE_DAYS <= (DateTime.Now.Day - stagedDate.Day))
                        {
                            if (record.RouteIdentifier != null || record.RouteIdentifier.Length > 0)
                            {
                                Logger.DebugFormat("Route {0} with  Order {1} Staged date is less than {2} day, Record removed from database", record.RouteIdentifier, record.OrderIdentifier, Config.ARCHIVE_DAYS);
                                _IntegrationDBAccessor.DeleteExpiredStagedRouteAndOrder(record.RegionIdentifier, record.OrderIdentifier,
                                record.Status, record.Staged, record.RouteIdentifier, out errorMessage, out errorCaught);
                                Logger.DebugFormat("Removal of Route {0} ,  Order {1} completed successfully", record.RouteIdentifier, record.OrderIdentifier);
                            }
                            else
                            {
                                Logger.DebugFormat("Order {1} Staged date is less than {2} day, Record removed from database", record.OrderIdentifier, Config.ARCHIVE_DAYS);
                                _IntegrationDBAccessor.DeleteExpiredStagedRouteOrder(record.RegionIdentifier, record.OrderIdentifier,
                               record.Status, record.Staged, out errorMessage, out errorCaught);
                                Logger.DebugFormat("Removal of Order {1} From Staged Route Table completed successfully", record.RouteIdentifier, record.OrderIdentifier);
                            }
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
