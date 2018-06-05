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
        private DictCache dictCache = new DictCache();
        private static CacheHelper cacheHelper = new CacheHelper();
        private string dictCacheFile = string.Empty;
        public static DateTime lastSuccessfulRunTime = new DateTime();



        public RegionProcessor(Region region) : base(MethodBase.GetCurrentMethod().DeclaringType, region.Identifier)
        {
            _Region = region;
            _ApexConsumer = new ApexConsumer(region, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
            dictCacheFile = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Dicts_" + _Region.Identifier + ".json");


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
                            WriteDictCachedData();
                            LoadDictCachedData();
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
                        LoadDictCachedData();
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
                    Logger.Debug("Start Retrieving and Saving Region " + _Region.Identifier + " Service Locations");
                    //Service location Processing
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
        private void WriteDictCachedData()
        {



            ApexConsumer.ErrorLevel errorLevel = ApexConsumer.ErrorLevel.None;
            string errorMessage = string.Empty;


            Logger.InfoFormat("Writing Dictionary Cache file to {0}", dictCacheFile);


            try
            {
                dictCache.depotsForRegionDict = _ApexConsumer.RetrieveDepotsForRegion(out errorLevel, out errorMessage, _Region.EntityKey);
                dictCache.orderClassesDict = _ApexConsumer.RetrieveOrderClassesDict(out errorLevel, out errorMessage);
                dictCache.regionEntityKeyDict = _ApexConsumer.RetrieveRegionEntityKey(out errorLevel, out errorMessage);
                dictCache.serviceTimeEntityKeyDict = _ApexConsumer.RetrieveServiceTimeEntityKey(out errorLevel, out errorMessage);
                dictCache.timeWindowEntityKeyDict = _ApexConsumer.RetrieveTimeWindowEntityKey(out errorLevel, out errorMessage);

                Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings
                {
                    PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects,
                    ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore

                };



                string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(dictCache, Newtonsoft.Json.Formatting.None, settings);


                // StreamWriter writer = new StreamWriter(dictCacheFile, append: false);
                System.IO.File.WriteAllText(dictCacheFile, jsonData);



                // writer.Write(jsonData);

            }


            catch (Exception ex)
            {
                Logger.ErrorFormat("Error writing cache file: {0}", ex.Message);
            }


        }

        private void LoadDictCachedData()
        {
            if (!File.Exists(dictCacheFile))
            {
                Logger.Info("No cache file exists");
            }
            else
            {
                Logger.InfoFormat("Loading cache file from {0}", dictCacheFile);
                try
                {
                    string jsonData = File.ReadAllText(dictCacheFile);
                    DictCache temp = Newtonsoft.Json.JsonConvert.DeserializeObject<DictCache>(jsonData);
                    if (temp != null)
                    {
                        dictCache = temp;
                        Logger.Debug("Dicts loaded from " + dictCacheFile);
                    }
                }
                catch (Exception ex)
                {
                    Logger.ErrorFormat("Error opening cache file: {0}", ex.Message);
                }
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

        private List<ServiceLocation> RetrieveNewSLToSave(string regionID)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            List<ServiceLocation> newServiceLocations = new List<ServiceLocation>();

            newServiceLocations = RetrieveNewSLRecords(out errorCaught, out errorMessage, regionID);


            if (newServiceLocations == null)
            {
                newServiceLocations = new List<ServiceLocation>();
                return newServiceLocations;
            }


            return newServiceLocations;
        }

        private List<ServiceLocation> RetrieveNewSLRecords(out bool errorRetrieveSLFromStagingTable, out string errorRetrieveSLFromStagingTableMessage, string regionID)
        {
            List<ServiceLocation> newServiceLocations = new List<ServiceLocation>();
            errorRetrieveSLFromStagingTable = false;
            errorRetrieveSLFromStagingTableMessage = string.Empty;
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


                }
                else
                {
                    errorRetrieveSLFromStagingTable = false;
                    return null;
                }

            }
            catch (Exception ex)
            {
                errorRetrieveSLFromStagingTable = true;
                errorRetrieveSLFromStagingTableMessage = ex.Message;
                Logger.Error("Error Retrieveing New SL's from Database: " + errorRetrieveSLFromStagingTableMessage);
            }
            return newServiceLocations;
        }

        private void SaveSLToRNA(string regionID, List<ServiceLocation> serviceLocations, out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;

            List<ServiceLocation> saveServiceLocations = serviceLocations.Where(sl => (sl.Action == ActionType.Add) || (sl.Action == ActionType.Update)).ToList();
            List<ServiceLocation> deleteServiceLocations = serviceLocations.Where(sl => (sl.Action == ActionType.Delete)).ToList();

            try
            {
                

            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }

        }

        private void SeperateOrders(string regionID, List<Order> orders, out bool errorCaught, out string errorMessage, out List<Order> updateOrders, out List<Order> newOrders)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            List<OrderSpec> orderSpecs = new List<OrderSpec>();

            List<string> orderIdentifiers = orders.Select(order => order.Identifier).ToList();
            newOrders = new List<Order>();
            updateOrders = new List<Order>();
            try
            {
                List<Order> ordersFromRNA = _ApexConsumer.RetrieveOrdersFromRNA(out errorCaught, out errorMessage, orderIdentifiers.ToArray());
                if (!errorCaught)
                {
                    updateOrders = orders.Intersect(ordersFromRNA).ToList();
                    newOrders = orders.Except(updateOrders).ToList();


                } else
                {
                    Logger.Error("Error Caught Checking if Orders Exist in RNA : " + errorMessage); 
                }

               foreach(Order updateOrder in updateOrders)
                {
                    Order databaseOrder = orders.Find(order => (order.Identifier == updateOrder.Identifier) && 
                    (order.BeginDate == updateOrder.BeginDate));

                    updateOrder.Action = ActionType.Update;
                    updateOrder.Tasks = databaseOrder.Tasks;
                    updateOrder.Tasks[0].ServiceWindowOverrides = ServiceWindowConsolidation(updateOrder, databaseOrder);

                });

                System.Threading.Tasks.Parallel.ForEach(updateOrders, (newOrder) =>
                {

                });


            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }

           
        
        }

        private OrderSpec ConvertToOrderSpec(string regionID, List<Order> orders, out bool errorCaught, out string errorMessage)
        {
            errorCaught = false;
            errorMessage = string.Empty;
            List<OrderSpec> orderSpecs = new List<OrderSpec>();

            List<string> orderIdentifiers = orders.Select(order => order.Identifier).ToList();
            List<Order> newOrder = new List<Order>();
            List<Order> updateOrder = new List<Order>();
            try
            {
                List<Order> ordersFromRNA = _ApexConsumer.RetrieveOrdersFromRNA(out errorCaught, out errorMessage, orderIdentifiers.ToArray());
                if (!errorCaught)
                {
                    updateOrder = orders.Intersect(ordersFromRNA).ToList();
                    newOrder = orders.Except(updateOrder).ToList();


                }
                else
                {
                    Logger.Error("Error Caught Checking if Orders Exist in RNA : " + errorMessage);
                }


            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }



        }

        private List<Order> RetrieveOrdersSave(string regionID)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            List<Order> newOrders = new List<Order>();

            newOrders = RetrieveNewOrderRecords(out errorCaught, out errorMessage, regionID);
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

        private List<Order> RetrieveNewOrderRecords(out bool errorRetrieveOrdersFromTable, out string errorRetrieveOrdersFromTableMessage, string regionID)
        {
            List<Order> newOrders = new List<Order>();
            errorRetrieveOrdersFromTable = false;
            errorRetrieveOrdersFromTableMessage = string.Empty;
            try
            {

                List<DBAccess.Records.StagedOrderRecord> retrieveOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
                retrieveOrderRecords = _IntegrationDBAccessor.RetrievedStagedOrders(regionID);
                if (retrieveOrderRecords != null)
                {

                    newOrders = retrieveOrderRecords.Cast<Order>().ToList();

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

        
    }
}
