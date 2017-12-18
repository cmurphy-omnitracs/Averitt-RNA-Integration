using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using DBAccessUtility;
using Averitt_RNA.DBAccess.Records;

namespace Averitt_RNA.DBAccess
{
    public class IntegrationDBAccessor : DBAccessor
    {

        #region Private Members

        private log4net.ILog _Logger;

        #endregion

        #region Public Methods

        public IntegrationDBAccessor(log4net.ILog logger)
            : base(ConfigurationManager.ConnectionStrings["INTEGRATION"].ConnectionString)
        {
            _Logger = logger;
        }

        public List<StagedOrderRecord> SelectStagedOrders(string regionID, string deleteBit)
        {
            List<StagedOrderRecord> stagedOrderRecordList = null;
            try
            {
                stagedOrderRecordList =
                    GetList(
                        SQLStrings.SELECT_STAGED_ORDERS(regionID, deleteBit),
                        new StagedOrderRecord(),
                        "Select Staged Orders (" + regionID + " " + deleteBit+")"
                    ).Cast<StagedOrderRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }
            return stagedOrderRecordList;
        }
        public List<StagedRouteRecord> SelectStagedRoutesStatus(string status, out string databaseError, out bool databaseErrorCaught)
        {
            List<StagedRouteRecord> stagedRouteRecordList = null;
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                stagedRouteRecordList =
                    GetList(
                        SQLStrings.SELECT_ALL_STAGED_ROUTES_STATUS(status),
                        new StagedRouteRecord(),
                        "Select Staged routes (" + status + ")"
                    ).Cast<StagedRouteRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }
            return stagedRouteRecordList;
        }

        public List<StagedServiceLocationRecord> SelectStagedServiceLocations(string regionID)
        {
            List<StagedServiceLocationRecord> stagedStagedServiceLocationList = null;
            string status = "NEW";
            try
            {
                stagedStagedServiceLocationList =
                    GetList(
                        SQLStrings.SELECT_STAGED_SERVICE_LOCATIONS(regionID, status),
                        new StagedServiceLocationRecord(),
                        "Select Staged Service Location (" + regionID + ")"
                    ).Cast<StagedServiceLocationRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
               
            }



            _Logger.Debug("Sucessfully Retrieved Service Locations " + stagedStagedServiceLocationList.Count +" from Staged_Service_Location Table" );
            return stagedStagedServiceLocationList;
        }

        public List<StagedServiceLocationRecord> SelectAllStagedServiceLocations(string regionID)
        {
            List<StagedServiceLocationRecord> stagedStagedServiceLocationList = null;
            
            try
            {
                stagedStagedServiceLocationList =
                    GetList(
                        SQLStrings.SELECT_ALL_NEW_STAGED_SERVICE_LOCATIONS(regionID),
                        new StagedServiceLocationRecord(),
                        "Select Staged Service Location (" + regionID + ")"
                    ).Cast<StagedServiceLocationRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);

            }



            _Logger.DebugFormat("Sucessfully Retrieved {0} Service Locations from Staged_Service_Location Table", stagedStagedServiceLocationList.Count);
            return stagedStagedServiceLocationList;
        }

        public List<StagedServiceLocationRecord> SelectAllStagedServiceLocationsStatus(string status, out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            List<StagedServiceLocationRecord> stagedStagedServiceLocationList = null;

            try
            {
                stagedStagedServiceLocationList =
                    GetList(
                        SQLStrings.SELECT_ALL_STAGED_SERVICE_LOCATIONS_STATUS(status),
                        new StagedServiceLocationRecord(),
                        "Select Staged Service Location with Status(" + status + ")"
                    ).Cast<StagedServiceLocationRecord>().ToList();
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);

            }



            _Logger.DebugFormat("Sucessfully Retrieved Service Locations with status {0} from Staged_Service_Location Table", status);
            return stagedStagedServiceLocationList;
        }

        public void InsertStagedRoute(string orderNumber, string regionID,  string routeId, string routeStartTime, string RouteDescr, string stopSeq, string staged, string error, string status)
        {
           
            try
            {
                ExecuteNonQuery(
                    SQLStrings.INSERT_STAGED_ROUTES(orderNumber, regionID, routeId, routeStartTime, RouteDescr, stopSeq, staged, error, status),
                     "Insert Route " + routeId + " into Staged Route table for Region " + regionID);
            }
            catch (DatabaseException ex)
            {
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }
         
            
        }

        public void InsertStagedUnassignedOrders(string regionID, string orderId, string staged, string error, string status, out string databaseError, out bool errorCaught)
        {
            databaseError = string.Empty;
            errorCaught = false;
            try
            {
                ExecuteNonQuery(
                    SQLStrings.INSERT_STAGED_ROUTES_UNASSIGNED_ORDER(regionID, orderId, staged, status),
                     "Insert UnnasignedOrder " + orderId + " into Staged Route table for Region " + regionID);
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                errorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
                
            }

        }


        //public void DeleteServiceLocation(string regionID, string serviceLocationId, string staged, string error, string status, out string databaseError, out bool databaseErrorCaught)
        //{
        //    databaseError = string.Empty;
        //    databaseErrorCaught = false;
        //    try
        //    {
        //        ExecuteNonQuery(
        //            SQLStrings.UPDATE_STAGED_SERVICE_LOCATION(regionID, serviceLocationId, staged, error, status),
        //             "Update  Service Location " + serviceLocationId + " status from New to Completed");
        //    }
        //    catch (DatabaseException ex)
        //    {
        //        databaseError = ex.Message;
        //        databaseErrorCaught = true;
        //        _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
        //    }

        //}

        public void UpdateOrderStatus(string regionID, string OrderId, string error, string status,
       out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 

                ExecuteNonQuery(
                    SQLStrings.UPDATE_STAGED_ORDERS_STATUS(regionID, OrderId, error, status),
                     "Update  Service Location " + OrderId + " status from New to Completed");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        public void UpdateServiceLocationStatus(string regionID, string serviceLocationID, string error, string status,
            out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 
               
                ExecuteNonQuery(
                    SQLStrings.UPDATE_STAGED_SERVICE_LOCATION_STATUS(regionID, serviceLocationID, error, status),
                     "Update  Service Location " + serviceLocationID + " status from New to Completed");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        public void DeleteExpiredOrder(string regionID, string orderId, string status, string staged, out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 
                ExecuteNonQuery(
                    SQLStrings.DELETE_EXPIRED_STAGED_ORDERS(regionID, orderId, staged, status),
                     "Delete expired Order" + orderId + " from STAGE_ORDER Table");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        public void DeleteExpiredStagedRoute(string regionID, string orderId, string status, string staged, out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 
                ExecuteNonQuery(
                    SQLStrings.DELETE_EXPIRED_STAGED_ROUTES(regionID, status, staged, orderId),
                     "Delete expired Route" + orderId + " from STAGED_ROUTE Table");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        public void DeleteExpiredStagedServiceLocation(string regionID, string serviceLocationId, string staged, out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 
                ExecuteNonQuery(
                    SQLStrings.DELETE_EXPIRED_STAGED_SERVICE_LOCATION(regionID, serviceLocationId, staged),
                     "Delete expired Service Location" + serviceLocationId + " from STAGED_SERVICE_LOCATION Table");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        public List<StagedOrderRecord> SelectAllStagedOrdersStatus(string status, out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            List<StagedOrderRecord> stagedOrderRecords = null;
            try
            {
                //Edit this 

                stagedOrderRecords =
                   GetList(
                       SQLStrings.SELECT_ALL_STAGED_ORDERS_STATUS(status),
                       new StagedOrderRecord(),
                       "Select all staged orders based on Status: " + status + " from STAGED_ORDERS Table"
                   ).Cast<StagedOrderRecord>().ToList();

                
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }
            return stagedOrderRecords;
        }

        public void DeleteDuplicatedServiceLocation(out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 
                ExecuteNonQuery(
                    SQLStrings.DELETE_DUPLICATE_SERVICE_LOCATIONS(),
                     "Delete Duplicate Service Locations from STAGED_SERVICE_LOCATION Table");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        public void DeleteDuplicatedOrders(out string databaseError, out bool databaseErrorCaught)
        {
            databaseError = string.Empty;
            databaseErrorCaught = false;
            try
            {
                //Edit this 
                ExecuteNonQuery(
                    SQLStrings.DELETE_DUPLICATE_ORDERS(),
                     "Delete Duplicate ORDERS from STAGED_ORDERS Table");
            }
            catch (DatabaseException ex)
            {
                databaseError = ex.Message;
                databaseErrorCaught = true;
                _Logger.Error("IntegrationDBAccessor | " + ex.Message, ex);
            }

        }

        #endregion

    }
}
