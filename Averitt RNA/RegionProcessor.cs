using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Averitt_RNA.Apex;
using Averitt_RNA.DBAccess;
using WindowsServiceUtility;
using System.Threading;
using System.IO;

namespace Averitt_RNA
{
    class RegionProcessor : Processor
    {

        private Region _Region;
        private ApexConsumer _ApexConsumer;
        private IntegrationDBAccessor _IntegrationDBAccessor;
        private DictCache dictCache;
        private static CacheHelper cacheHelper = new CacheHelper();
        private string dictCacheFile = string.Empty;
        public static DateTime lastSuccessfulRunTime = new DateTime();



        public RegionProcessor(Region region) : base(MethodBase.GetCurrentMethod().DeclaringType, region.Identifier)
        {
            _Region = region;
            _ApexConsumer = new ApexConsumer(region, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
            dictCacheFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Dicts_" + _Region.Identifier + ".json");
            dictCache = new DictCache(dictCacheFile, Logger);

        }



        public override void Process()
        {

            if (!MainService.SessionRequired)
            {
                bool errorCaught = false;
                string errorMessage = string.Empty;
                string fatalErrorMessage = string.Empty;
                bool timeOut = false;

                string successfullRunCacheFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), String.Format("{0}-SuccessfulRunTimeCache.json", _Region.Identifier));

                //Get last successfull Rune Time
                Logger.Info("Retrieve Last Successful Run Time");
                if (!File.Exists(successfullRunCacheFile))
                {
                    Logger.Info("No run time cache file exists");
                    WriteSuccessfullRunTimeCache(successfullRunCacheFile);
                }
                else
                {
                    LoadRunTimeCache(successfullRunCacheFile);
                }

                Logger.Debug("Start Retrieving Region Cache Files");

                //Write cache file if it doesn't exist or if it needs to get refreshed
                if (((DateTime.Now.Minute % Config.DictServiceTimeRefresh) == 0) || !File.Exists(dictCacheFile))
                {
                    try
                    {
                        Object thisLock = new Object();

                        Logger.Debug("Starting Writing and Loading of Dictionaries");
                        lock (thisLock)
                        {
                            dictCache.resetCache();
                            dictCache.WriteDictCachedData(_Region.EntityKey);
                            dictCache.LoadDictCachedData();
                        }
                        Logger.Debug("Writing and Loading Dictionaries Completed Successfully");
                    }


                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("Error Loading or Writing Dictionary Cache File: {0}", ex.Message);
                    }



                }
                else
                {

                    //Load Caches

                    try
                    {
                        Logger.Debug("Starting Loading of Dictionaries");
                        dictCache.LoadDictCachedData();
                        Logger.Debug("Loading Dictionaries Completed Successfully");
                    }


                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("Error Loading or Writing Dictionary Cache File: {0}", ex.Message);
                    }

                }

                try
                {
                    //Region Processing
                    Logger.Info("Start Retrieving and Saving Region " + _Region.Identifier + " Service Locations");
                    //Service location Processing

                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Start Retrieving NEW Service Locations from staging table");
                    List<ServiceLocation> newServiceLocations = RetrieveNewSLRecords(_Region.Identifier);
                    Logger.InfoFormat("Retrieved {0} New/Updated ServiceLocationsRecords Succesfull", newServiceLocations.Count());
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Saving {0} New/Updated Service Locations to RNA", newServiceLocations.Count());
                    SaveSLToRNA(_Region.Identifier, newServiceLocations);
                    Logger.InfoFormat("Service Locations Save Process Finished", newServiceLocations.Count());
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    //New Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Start Retrieving NEW Orders from staging table");
                    List<DBAccess.Records.StagedOrderRecord> dbOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
                    List<Order> newDBOrders = RetrieveOrdersSave(_Region.Identifier, out dbOrderRecords);
                    Logger.InfoFormat("Retrieved {0} Orders from staging table successfully", newDBOrders.Count);
                    Logger.InfoFormat("Seperate Order Types and prepare for saving");
                    List<Order> updateOrders = new List<Order>();
                    List<Order> newOrders = new List<Order>();
                    List<Order> deleteOrders = new List<Order>();
                    SeperateOrders(_Region.Identifier, newDBOrders, out updateOrders, out newOrders, out deleteOrders);
                    Logger.InfoFormat("Seperation and Order Processing Successful");
                    Logger.InfoFormat("Create Routing Session for new Orders into RNA");

                    CreateRoutingSessions(dbOrderRecords.Where(order => newOrders.Any(newRnaOrder => newRnaOrder.Identifier==order.OrderIdentifier)).ToList(), newOrders);

                    Logger.InfoFormat("Find Routes in Routing Sessions");

                    _ApexConsumer.RetrieveSLFromSTandSaveToRNA(dictCache.regionEntityKeyDict, dictCache.timeWindowEntityKeyDict, dictCache.serviceTimeEntityKeyDict,
                      _Region.Identifier, out errorCaught, out errorMessage, out fatalErrorMessage, out timeOut);

                    //Pick Up Dummy Order Processing
                    _ApexConsumer.RetrieveDummyOrdersAndSave(dictCache.depotsForRegionDict, dictCache.orderClassesDict, _Region.Identifier, out errorCaught, out errorMessage);

                    //Orders Processing * correct Save Order Result

                    _ApexConsumer.RetrieveOrdersandSaveToRNA(dictCache.regionEntityKeyDict, dictCache.depotsForRegionDict, dictCache.orderClassesDict,
                       _Region.Identifier, out errorCaught, out errorMessage, out fatalErrorMessage, out timeOut);



                    //Write Routes and Unassigned
                    _ApexConsumer.RetrieveRNARoutesAndOrdersWriteThemToStagingTable(out errorCaught, out errorMessage);

                    Logger.Debug("Retrieving Region Processing Completed Successfully");
                    WriteSuccessfullRunTimeCache(successfullRunCacheFile);
                }
                catch (Exception ex)
                {
                    Logger.ErrorFormat("Error Processing Region: {0}", ex.Message);
                }


            }
            else
            {
                Logger.Info("Waiting for Session.");
            }


        }
      
        private void WriteSuccessfullRunTimeCache(string filename)
        {



            string errorMessage = string.Empty;


            Logger.InfoFormat("Writing Timestamp of Lasts Successful processing for region {1} Cache file to {0}", filename, _Region.Identifier);
            try
            {



                using (StreamWriter writer = new StreamWriter(filename, append: false))
                {
                    Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings
                    {
                        PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.None,
                        ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore
                    };

                    string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(DateTime.Now, Newtonsoft.Json.Formatting.None, settings);

                    writer.Write(jsonData);
                }


            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error writing Successful Runtime cache file: {0}", ex.Message);
            }
        }

        private void LoadRunTimeCache(string filename)
        {
            if (!File.Exists(filename))
            {
                Logger.Info("No Run Time cache file exists");
            }
            else
            {
                Logger.InfoFormat("Loading cache file from {0}", filename);
                try
                {
                    string jsonData = File.ReadAllText(filename);
                    string temp = Newtonsoft.Json.JsonConvert.DeserializeObject<string>(jsonData);
                    DateTime temp2 = new DateTime();
                    if (temp != null && DateTime.TryParse(temp, out temp2))
                    {
                        lastSuccessfulRunTime = temp2;
                        Logger.Debug("Run Time successfully loaded from " + filename);
                        Logger.Debug("Last Successful Run Time " + lastSuccessfulRunTime.ToString());
                    }
                }
                catch (Exception ex)
                {
                    Logger.ErrorFormat("Error opening run time cache file: {0}", ex.Message);
                }
            }
        }

       private List<ServiceLocation> RetrieveNewSLRecords(string regionID)
        {
            List<ServiceLocation> newServiceLocations = new List<ServiceLocation>();
            bool errorCaught = false;
            string errorSLMessage = string.Empty;
            try
            {

                List<DBAccess.Records.StagedServiceLocationRecord> retrieveNewSLRecords = new List<DBAccess.Records.StagedServiceLocationRecord>();
                retrieveNewSLRecords = _IntegrationDBAccessor.SelectNewStagedServiceLocations(regionID);
                if (retrieveNewSLRecords != null)
                {



                    List<DBAccess.Records.StagedServiceLocationRecord> errorSLRecords = retrieveNewSLRecords.Where(sLrecord => (sLrecord.ServiceLocationIdentifier == null || sLrecord.ServiceLocationIdentifier.Length == 0) ||
                    (sLrecord.AddressLine1 == null || sLrecord.AddressLine1.Length == 0) ||
                    (sLrecord.RegionIdentifier == null || sLrecord.RegionIdentifier.Length == 0) ||
                    (sLrecord.WorldTimeZone == null || sLrecord.WorldTimeZone.Length == 0) ||
                    (sLrecord.ServiceTimeTypeIdentifier == null || sLrecord.ServiceTimeTypeIdentifier.Length == 0) ||
                    (sLrecord.ServiceWindowTypeIdentifier == null || sLrecord.ServiceWindowTypeIdentifier.Length == 0)
                    ).ToList();

                    newServiceLocations = retrieveNewSLRecords.FindAll(sl => !errorSLRecords.Contains(sl)).DefaultIfEmpty(new DBAccess.Records.StagedServiceLocationRecord()).Cast<ServiceLocation>().ToList();

                    //update service locations in errorlist in database indicating service location is missing fields
                    foreach (DBAccess.Records.StagedServiceLocationRecord sLrecord in errorSLRecords)
                    {
                        errorCaught = false;
                        errorSLMessage = string.Empty;
                        string errorMessage = string.Empty;
                        if (sLrecord.AddressLine1 == null || sLrecord.AddressLine1.Length == 0)
                        {
                            errorMessage = errorMessage + "AddressLine1 is null or empty | ";

                        }
                        if (sLrecord.ServiceLocationIdentifier == null || sLrecord.ServiceLocationIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "ServiceLocationIdentifier is null or empty | ";

                        }
                        if (sLrecord.RegionIdentifier == null || sLrecord.RegionIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "RegionIdentifier is null or empty | ";

                        }
                        if (sLrecord.WorldTimeZone == null || sLrecord.WorldTimeZone.Length == 0)
                        {
                            errorMessage = errorMessage + "WorldTimeZone is null or empty | ";

                        }
                        if (sLrecord.ServiceTimeTypeIdentifier == null || sLrecord.ServiceTimeTypeIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "ServiceTimeTypeIdentifier is null or empty | ";

                        }
                        if (sLrecord.ServiceWindowTypeIdentifier == null || sLrecord.ServiceWindowTypeIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "ServiceWindowTypeIdentifier is null or empty | ";

                        }

                        _IntegrationDBAccessor.UpdateServiceLocationStatus(sLrecord.RegionIdentifier, sLrecord.ServiceLocationIdentifier, errorMessage, "Error", out errorSLMessage, out errorCaught);
                        if (errorCaught)
                        {
                            Logger.Error("Error Updating SL " + sLrecord.ServiceLocationIdentifier + " with Error Status | " + errorMessage);

                        }
                        else
                        {
                            Logger.Debug("Service Location " + sLrecord.ServiceLocationIdentifier + " error status update successfully");
                        }
                    }



                }
                else
                {
                    errorCaught = false;
                    return null;
                }

            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorSLMessage = ex.Message;
                Logger.Error("Error Retrieveing New SL's from Database: " + errorSLMessage);
            }
            return newServiceLocations;
        }

       private void SaveSLToRNA(string regionID, List<ServiceLocation> serviceLocations)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            try
            {
                SaveResult[] slSaveResult = _ApexConsumer.SaveRNAServiceLocations(out errorCaught, out errorMessage, serviceLocations.ToArray());
                if (!errorCaught)
                {
                    if (slSaveResult != null && slSaveResult.Count() > 0)
                    {
                        foreach (SaveResult saveResult in slSaveResult)
                        {
                            ServiceLocation serviceLocation = (ServiceLocation)saveResult.Object;
                            errorCaught = false;
                            errorMessage = string.Empty;
                            if (saveResult.Error != null)
                            {

                                if (saveResult.Error.Code.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {
                                        Logger.Debug("A Validation Error Occured While Saving Service Locations. The " + validFailure.Property + " Property for Order " + serviceLocation.Identifier + " is not Valid");
                                        Logger.Debug("Updating Service Location " + serviceLocation.Identifier + " db record status to Error");
                                        _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, serviceLocation.Identifier, "Validation Error For Properties " + validFailure.Property + "See Log", "ERROR", out errorMessage, out errorCaught);
                                        if (errorCaught)
                                        {
                                            Logger.Debug("Updating Service Location " + serviceLocation.Identifier + " error status in staging table failed | " + errorMessage);

                                        }
                                        else
                                        {
                                            Logger.Debug("Updating Service Location " + serviceLocation.Identifier + " error status succesfull");
                                        }
                                    }
                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Saving Service Locations. The " + saveResult.Error.Code.ErrorCode_Status + " Order " + serviceLocation.Identifier + " is not Valid");
                                    Logger.Debug("Updating Service Location " + serviceLocation.Identifier + " db records status to Error");
                                    _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, serviceLocation.Identifier, "Error: " + saveResult.Error.Code.ErrorCode_Status + " See Log", "ERROR", out errorMessage, out errorCaught);
                                    if (errorCaught)
                                    {
                                        Logger.Debug("Updating Service Location " + serviceLocation.Identifier + " error status in staging table failed | " + errorMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Order " + serviceLocation.Identifier + " error status succesfull");
                                    }

                                }
                            }
                            else
                            {
                                Logger.Debug("Saving/Updating Order : " + serviceLocation.Identifier + " to RNA Successfull ");
                            }

                        }

                    }

                }
                else
                {

                }


            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }

        }

       private void SeperateOrders(string regionID, List<Order> orders, out List<Order> updateOrders, out List<Order> newOrders, out List<Order> deleteOrders)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            List<OrderSpec> orderSpecs = new List<OrderSpec>();

            List<string> orderIdentifiers = orders.Select(order => order.Identifier).ToList();
            newOrders = new List<Order>();
            updateOrders = new List<Order>();
            deleteOrders = new List<Order>();
            try
            {
                List<Order> ordersFromRNA = _ApexConsumer.RetrieveOrdersFromRNA(out errorCaught, out errorMessage, orderIdentifiers.ToArray());
                if (!errorCaught)
                {
                    updateOrders = orders.Where(order => order.Action != ActionType.Delete).Intersect(ordersFromRNA).ToList();
                    newOrders = orders.Where(order => order.Action != ActionType.Delete).Except(updateOrders).ToList();
                    deleteOrders = orders.Where(order => order.Action == ActionType.Delete).Intersect(ordersFromRNA).ToList();

                }
                else
                {
                    Logger.Error("Error Caught Checking if Orders Exist in RNA : " + errorMessage);
                }

                foreach (Order updateOrder in updateOrders)
                {
                    Order databaseOrder = orders.Find(order => (order.Identifier == updateOrder.Identifier) &&
                    (order.BeginDate == updateOrder.BeginDate));

                    updateOrder.Action = ActionType.Update;
                    updateOrder.Tasks = databaseOrder.Tasks;
                    updateOrder.Tasks[0].ServiceWindowOverrides = ServiceWindowConsolidation(updateOrder, databaseOrder);

                }
                if (newOrders == null) { newOrders = new List<Order>(); }
                if (deleteOrders == null) { deleteOrders = new List<Order>(); }


            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }



        }

        private void SaveUpdateRNAOrders(string regionID, List<Order> orders, out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;

            System.Collections.Concurrent.ConcurrentBag<OrderSpec> orderSpecs = new System.Collections.Concurrent.ConcurrentBag<OrderSpec>();

            System.Threading.Tasks.Parallel.ForEach(orders, (rnaOrder) =>
            {
                orderSpecs.Add(ConvertOrderToOrderSpec(rnaOrder));

            });


            try
            {
                List<SaveResult> saveOrdersResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, orderSpecs.ToArray()).ToList();
                if (!errorCaught)
                {
                    foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error != null)).ToList())
                    {
                        var tempOrder = (Order)saveResult.Object;
                        bool errorUpdatingServiceLocation = false;
                        string errorUpdatingServiceLocationMessage = string.Empty;
                        if (saveResult.Error != null)
                        {


                            if (saveResult.Error.Code.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                            {
                                foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                {
                                    Logger.Debug("A Validation Error Occured While Saving Orders. The " + validFailure.Property + " Property for Order " + tempOrder.Identifier + " is not Valid");
                                    Logger.Debug("Updating Order " + tempOrder.Identifier + " db record status to Error");
                                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, tempOrder.Identifier, "Validation Error For Properties " + validFailure.Property + "See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                    if (errorUpdatingServiceLocation)
                                    {
                                        Logger.Debug("Updating Order " + tempOrder.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Order " + tempOrder.Identifier + " error status succesfull");
                                    }
                                }
                            }
                            else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                            {

                                Logger.Debug("An Error Occured While Saving Orders. The " + saveResult.Error.Code.ErrorCode_Status + " Order " + tempOrder.Identifier + " is not Valid");
                                Logger.Debug("Updating Order " + tempOrder.Identifier + " db records status to Error");
                                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, tempOrder.Identifier, "Error: " + saveResult.Error.Code.ErrorCode_Status + " See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                if (errorUpdatingServiceLocation)
                                {
                                    Logger.Debug("Updating Order " + tempOrder.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                }
                                else
                                {
                                    Logger.Debug("Updating Order " + tempOrder.Identifier + " error status succesfull");
                                }

                            }
                        }
                        else
                        {
                            Logger.Debug("Saving/Updating Order : " + tempOrder.Identifier + " to RNA Successfull ");
                        }
                    }

                }
                else
                {
                    Logger.Error("Error Caught Saving Orders to RNA : " + errorMessage);

                }


            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }



        }

        private List<Order> RetrieveOrdersSave(string regionID, out List<DBAccess.Records.StagedOrderRecord> stagedOrderRecords)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            List<Order> newOrders = new List<Order>();
            stagedOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
            newOrders = RetrieveNewOrderRecords(out errorCaught, out errorMessage, out stagedOrderRecords, regionID);
            if (!errorCaught)
            {

                if (newOrders.Count == 0)
                {

                    Logger.Info("No New Orders Found In Database");
                    return new List<Order>();

                }
                else
                {
                    return newOrders;

                }

            }
            else
            {
                Logger.Error("Error Ocurred Retrieving Orders form Database " + errorMessage);
                return new List<Order>();

            }


        }

        private List<Order> RetrieveNewOrderRecords(out bool errorRetrieveOrdersFromTable, out string errorRetrieveOrdersFromTableMessage, out List<DBAccess.Records.StagedOrderRecord> stagedOrderRecords, string regionID)
        {
            List<Order> newOrders = new List<Order>();
            errorRetrieveOrdersFromTable = false;
            errorRetrieveOrdersFromTableMessage = string.Empty;
            stagedOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
            try
            {

                List<DBAccess.Records.StagedOrderRecord> retrieveOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
                retrieveOrderRecords = _IntegrationDBAccessor.RetrievedStagedOrders(regionID);
                if (retrieveOrderRecords != null)
                {

                    List<DBAccess.Records.StagedOrderRecord> errorOrderRecords = retrieveOrderRecords.Where(orderRecord => (orderRecord.OrderIdentifier == null || orderRecord.OrderIdentifier.Length == 0) ||
                  (orderRecord.BeginDate == null || orderRecord.BeginDate.Length == 0) ||
                  (orderRecord.RegionIdentifier == null || orderRecord.RegionIdentifier.Length == 0) ||
                  (orderRecord.OrderClassIdentifier == null || orderRecord.OrderClassIdentifier.Length == 0) ||
                  (orderRecord.QuantitySize1 == null || orderRecord.QuantitySize1.Length == 0) ||
                  (orderRecord.QuantitySize2 == null || orderRecord.QuantitySize2.Length == 0) ||
                  (orderRecord.QuantitySize3 == null || orderRecord.QuantitySize3.Length == 0) ||
                  (orderRecord.PreferredRouteIdentifier == null || orderRecord.PreferredRouteIdentifier.Length == 0) ||
                  (orderRecord.OriginDepotIdentifier == null || orderRecord.OriginDepotIdentifier.Length == 0)).ToList();

                    stagedOrderRecords = retrieveOrderRecords.FindAll(orderRecord => !errorOrderRecords.Contains(orderRecord)).DefaultIfEmpty(new DBAccess.Records.StagedOrderRecord()).ToList();
                    newOrders = stagedOrderRecords.Cast<Order>().ToList();
                    errorRetrieveOrdersFromTable = false;

                    foreach (DBAccess.Records.StagedOrderRecord orderRecord in errorOrderRecords)
                    {
                        bool errorCaught = false;
                        string errorSLMessage = string.Empty;
                        string errorMessage = string.Empty;
                        if (orderRecord.BeginDate == null || orderRecord.BeginDate.Length == 0)
                        {
                            errorMessage = errorMessage + "BeginDate is null or empty | ";

                        }
                        if (orderRecord.RegionIdentifier == null || orderRecord.RegionIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "RegionIdentifier is null or empty | ";

                        }
                        if (orderRecord.OrderClassIdentifier == null || orderRecord.OrderClassIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "OrderClass is null or empty | ";

                        }
                        if (orderRecord.QuantitySize1 == null || orderRecord.QuantitySize1.Length == 0)
                        {
                            errorMessage = errorMessage + "QuantitySize1 is null or empty | ";

                        }
                        if (orderRecord.QuantitySize2 == null || orderRecord.QuantitySize2.Length == 0)
                        {
                            errorMessage = errorMessage + "QuantitySize2 is null or empty | ";

                        }
                        if (orderRecord.QuantitySize3 == null || orderRecord.QuantitySize3.Length == 0)
                        {
                            errorMessage = errorMessage + "QuantitySize3 is null or empty | ";

                        } if (orderRecord.PreferredRouteIdentifier == null || orderRecord.PreferredRouteIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "PreferredRouteIdentifier is null or empty | ";
                        } if (orderRecord.OriginDepotIdentifier == null || orderRecord.OriginDepotIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "OriginDepotIdentifier is null or empty | ";
                        }

                        _IntegrationDBAccessor.UpdateOrderStatus(orderRecord.RegionIdentifier, orderRecord.OrderIdentifier, errorMessage, "Error", out errorSLMessage, out errorCaught);

                        if (errorCaught)
                        {
                            Logger.Error("Error Updating Order Record" + orderRecord.OrderIdentifier + " with Error Status | " + errorMessage);

                        }
                        else
                        {
                            Logger.Debug("Order Record " + orderRecord.OrderIdentifier + " error status update successfully");
                        }
                    }
                }
                else
                {
                    errorRetrieveOrdersFromTable = false;
                    return new List<Order>();
                }

            }
            catch (Exception ex)
            {
                errorRetrieveOrdersFromTable = true;
                errorRetrieveOrdersFromTableMessage = ex.Message;
                Logger.Error("Error Retrieveing New Orders from Database: " + errorRetrieveOrdersFromTableMessage);
            }
            return newOrders;
        }

        private List<Order> PrepareOrders(List<Order> unprepOrders)
        {
            List<Order> preppedOrder = new List<Order>();

            System.Threading.Tasks.Parallel.ForEach(preppedOrder, (order) => 
            {
                
            });

            return preppedOrder;

        }

        private void CreateRoutingSessions(List<DBAccess.Records.StagedOrderRecord> newOrderRecords, List<Order> newOrders)
        {
            List<Order> preppedOrder = new List<Order>();
            bool errorCaught = false;
            string errorMessage = string.Empty;

            System.Threading.Tasks.Parallel.ForEach(newOrderRecords, (order) =>
            {
                DateTime sessionDate = Convert.ToDateTime(order.BeginDate);
                DailyRoutingSession[] dailyRoutingSessions = _ApexConsumer.RetrieveDailyRoutingSessionwithOrigin(out errorCaught, out errorMessage, sessionDate, order.OriginDepotIdentifier);

                if (!errorCaught)
                {
                    if (dailyRoutingSessions != null || dailyRoutingSessions.Length == 0)
                    {
                        SaveResult[] dsSaveResult = _ApexConsumer.SaveDailyRoutingSessions(out errorCaught, out errorMessage, new DateTime[] { sessionDate }, new string[] { order.OriginDepotIdentifier });
                        if(dsSaveResult[0].Error == null)
                        {
                            DailyRoutingSession rtSession = (DailyRoutingSession)dsSaveResult[0].Object;
                            newOrders = newOrders.Where(newOrder => newOrder.Identifier == order.OrderIdentifier).Select(o => { o.SessionDate = order.BeginDate; o.SessionEntityKey = rtSession.EntityKey; return o; }).ToList();
                        } else
                        {
                            //TODO
                        }
                        
                    } else
                    {
                        newOrders = newOrders.Where(newOrder => newOrder.Identifier == order.OrderIdentifier).Select(o => { o.SessionDate = order.BeginDate; o.SessionEntityKey = dailyRoutingSessions[0].EntityKey; return o; }).ToList();
                    }
                }
                else
                {
                    //TODO
                }

            });

            return preppedOrder;

        }

        private TaskServiceWindowOverrideDetail[] ServiceWindowConsolidation(Order RNAOrder, Order dbOrder)
        {


            if ((RNAOrder.Tasks[0].ServiceWindowOverrides.Length == dbOrder.Tasks[0].ServiceWindowOverrides.Length) && (RNAOrder.Tasks[0].ServiceWindowOverrides != null && RNAOrder.Tasks[0].ServiceWindowOverrides.Length != 0))
            {
                for (int i = 0; i < RNAOrder.Tasks[0].ServiceWindowOverrides.Length; i++)
                {
                    RNAOrder.Tasks[0].ServiceWindowOverrides[i] = dbOrder.Tasks[0].ServiceWindowOverrides[i];
                    RNAOrder.Tasks[0].ServiceWindowOverrides[i].EntityKey = 0;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[i].Action = ActionType.Update;
                }
            }
            else if ((dbOrder.Tasks[0].ServiceWindowOverrides == null || dbOrder.Tasks[0].ServiceWindowOverrides.Length == 0) && RNAOrder.Tasks[0].ServiceWindowOverrides.Length > 0)
            {
                RNAOrder.Tasks[0].ServiceWindowOverrides = RNAOrder.Tasks[0].ServiceWindowOverrides.Select(sw => { sw.Action = ActionType.Delete; return sw; }).ToArray();

            }
            else
            {

                if (RNAOrder.Tasks[0].ServiceWindowOverrides.Length == 1 && dbOrder.Tasks[0].ServiceWindowOverrides.Length == 2)
                {
                    long entityKey1 = RNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[0] = dbOrder.Tasks[0].ServiceWindowOverrides[0];
                    RNAOrder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey = entityKey1;

                    RNAOrder.Tasks[0].ServiceWindowOverrides[1] = dbOrder.Tasks[0].ServiceWindowOverrides[1];
                    RNAOrder.Tasks[0].ServiceWindowOverrides[1].EntityKey = 0;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[1].Action = ActionType.Add;

                }
                else if (RNAOrder.Tasks[0].ServiceWindowOverrides.Length == 2 && dbOrder.Tasks[0].ServiceWindowOverrides.Length == 1)
                {
                    long entityKey1 = RNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[0] = dbOrder.Tasks[0].ServiceWindowOverrides[0];
                    RNAOrder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey = entityKey1;
                    RNAOrder.Tasks[0].ServiceWindowOverrides[1].Action = ActionType.Delete;

                }


            }


            return RNAOrder.Tasks[0].ServiceWindowOverrides;
        }

       private long CreateRoute(string createRouteID)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            SaveRouteArgs routeArgs = new SaveRouteArgs
            {
                Identifier = createRouteID,
                DispatcherEntityKey = ,
                OriginDepotEntityKey = (long)_Region.Defaults.DepotEntityKey,

                PassEntityKey = Config.DefaultRoutingPassEK,
                Phase = RoutePhase.Plan,
                Equipment = new RouteEquipmentType[]
                {
                   new RouteEquipmentType
                   {
                       EquipmentTypeEntityKey = (long)_Region.Defaults.EquipmentTypeEntityKey

                   }
               },



            };
            SaveRouteResult saveRouteResult = new SaveRouteResult();
            try
            {
                saveRouteResult = _ApexConsumer.CreateRNARoute(out errorCaught, out errorMessage, routeArgs);
                if (!errorCaught)
                {
                    if(saveRouteResult.Error != null)
                    {
                        //todo
                    } else
                    {
                        Logger.InfoFormat("Route {0} created successfully", routeArgs.Identifier);
                    }

                }else
                {
                    //todo
                }

            }catch (Exception ex)
            {
                //todo
            }

            return saveRouteResult.EntityKey;
        }

        private static OrderSpec ConvertOrderToOrderSpec(Order order)
        {
            OrderSpec orderSpec = new OrderSpec
            {
                BeginDate = order.BeginDate,
                CustomProperties = order.CustomProperties,
                EndDate = order.EndDate,
                ForceBulkServiceTime = order.ForceBulkServiceTime,
                Identifier = order.Identifier,
                LineItems = order.LineItems,
                ManagedByUserEntityKey = order.ManagedByUserEntityKey,
                NetRevenue = order.NetRevenue,
                OrderClassEntityKey = order.OrderClassEntityKey,
                OrderInstance = new DomainInstance
                {
                    EntityKey = order.EntityKey,
                    Version = order.Version
                },
                PlannedDeliveryCategory1Quantities = order.PlannedDeliveryCategory1Quantities,
                PlannedDeliveryCategory2Quantities = order.PlannedDeliveryCategory2Quantities,
                PlannedDeliveryCategory3Quantities = order.PlannedDeliveryCategory3Quantities,
                PlannedPickupCategory1Quantities = order.PlannedPickupCategory1Quantities,
                PlannedPickupCategory2Quantities = order.PlannedPickupCategory2Quantities,
                PlannedPickupCategory3Quantities = order.PlannedPickupCategory3Quantities,
                PreferredRouteIdentifierOverride = order.PreferredRouteIdentifierOverride,
                RegionEntityKey = order.RegionEntityKey,
                Selector = order.Selector,
                SessionEntityKey = order.SessionEntityKey,
                SpecialInstructions = order.SpecialInstructions,
                TakenBy = order.TakenBy,
                UploadSelector = order.UploadSelector

            };
            TaskSpecType taskSpecType = TaskSpecType.None;
            if (order.LineItems == null || order.LineItems.Length == 0)
            {
                if (order.Tasks != null && order.Tasks.Length != 0)
                {
                    if (order.Tasks.All(task => task.TaskType_Type == Enum.GetName(typeof(TaskType), TaskType.Delivery)))
                    {
                        taskSpecType = TaskSpecType.Delivery;
                    }
                    else if (order.Tasks.All(task => task.TaskType_Type == Enum.GetName(typeof(TaskType), TaskType.Pickup)))
                    {
                        taskSpecType = TaskSpecType.Pickup;
                    }
                    else
                    {
                        long firstTaskLocationEntityKey = order.Tasks.First().LocationEntityKey;
                        if (order.Tasks.All(task => task.LocationEntityKey == firstTaskLocationEntityKey))
                        {
                            taskSpecType = TaskSpecType.DeliveryAndPickup;
                        }
                        else
                        {
                            taskSpecType = TaskSpecType.Transfer;
                        }
                    }
                }
            }
            else
            {
                if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Delivery)))
                {
                    taskSpecType = TaskSpecType.Delivery;
                }
                else if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Pickup)))
                {
                    taskSpecType = TaskSpecType.Pickup;
                }
                else if (order.LineItems.All(lineItem => lineItem.LineItemType_Type == Enum.GetName(typeof(LineItemType), LineItemType.Transfer)))
                {
                    taskSpecType = TaskSpecType.Transfer;
                }
                else
                {
                    taskSpecType = TaskSpecType.DeliveryAndPickup;
                }
            }
            Task deliveryTask;
            Task pickupTask;
            switch (taskSpecType)
            {
                case TaskSpecType.Delivery:
                    orderSpec.TaskSpec = new DeliveryTaskSpec
                    {
                        AdditionalServiceTime = order.AdditionalServiceTime,
                        Quantities = order.PlannedDeliveryQuantities,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey

                    };
                    deliveryTask = order.Tasks.FirstOrDefault();
                    if (deliveryTask != null)
                    {
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).CoordinateOverride = deliveryTask.CoordinateOverride;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).OpenCloseOverrides = deliveryTask.OpenCloseOverrides;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).ServiceLocationEntityKey = deliveryTask.LocationEntityKey;
                        ((DeliveryTaskSpec)orderSpec.TaskSpec).ServiceWindowOverrides = deliveryTask.ServiceWindowOverrides;
                    }
                    break;
                case TaskSpecType.Pickup:
                    orderSpec.TaskSpec = new PickupTaskSpec
                    {
                        AdditionalServiceTime = order.AdditionalServiceTime,
                        Quantities = order.PlannedPickupQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,

                    };
                    pickupTask = order.Tasks.FirstOrDefault();
                    if (pickupTask != null)
                    {
                        ((PickupTaskSpec)orderSpec.TaskSpec).CoordinateOverride = pickupTask.CoordinateOverride;
                        ((PickupTaskSpec)orderSpec.TaskSpec).OpenCloseOverrides = pickupTask.OpenCloseOverrides;
                        ((PickupTaskSpec)orderSpec.TaskSpec).ServiceLocationEntityKey = pickupTask.LocationEntityKey;
                        ((PickupTaskSpec)orderSpec.TaskSpec).ServiceWindowOverrides = pickupTask.ServiceWindowOverrides;
                    }
                    break;
                case TaskSpecType.DeliveryAndPickup:
                    deliveryTask = order.Tasks[0];
                    pickupTask = order.Tasks[1];
                    orderSpec.TaskSpec = new DeliveryAndPickupTaskSpec
                    {
                        AdditionalServiceTime = deliveryTask.AdditionalServiceTime,
                        CoordinateOverride = deliveryTask.CoordinateOverride,
                        DeliveryQuantities = deliveryTask.PlannedQuantities,
                        OpenCloseOverrides = deliveryTask.OpenCloseOverrides,
                        PickupQuantities = pickupTask.PlannedQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey,
                        ServiceLocationEntityKey = deliveryTask.LocationEntityKey,
                        ServiceWindowOverrides = deliveryTask.ServiceWindowOverrides
                    };
                    break;
                case TaskSpecType.Transfer:
                    deliveryTask = order.Tasks[1];
                    pickupTask = order.Tasks[0];
                    orderSpec.TaskSpec = new TransferTaskSpec
                    {
                        DeliveryAdditionalServiceTime = TimeSpan.FromSeconds(order.AdditionalServiceTime.TotalSeconds / 2),
                        DeliveryCoordinateOverride = deliveryTask.CoordinateOverride,
                        DeliveryLocationEntityKey = deliveryTask.LocationEntityKey,
                        DeliveryOpenCloseOverrides = deliveryTask.OpenCloseOverrides,
                        DeliveryServiceWindowOverrides = deliveryTask.ServiceWindowOverrides,
                        PickupAdditionalServiceTime = TimeSpan.FromSeconds(order.AdditionalServiceTime.TotalSeconds / 2),
                        PickupCoordinateOverride = pickupTask.CoordinateOverride,
                        PickupLocationEntityKey = pickupTask.LocationEntityKey,
                        PickupOpenCloseOverrides = pickupTask.OpenCloseOverrides,
                        PickupServiceWindowOverrides = pickupTask.ServiceWindowOverrides,
                        Quantities = order.PlannedDeliveryQuantities,
                        RequiredDestinationEntityKey = order.RequiredRouteDestinationEntityKey,
                        RequiredOriginEntityKey = order.RequiredRouteOriginEntityKey
                    };
                    break;
            }
            return orderSpec;
        }





    }
}
