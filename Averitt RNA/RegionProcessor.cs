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
        public enum TaskSpecType
        {
            None,
            Delivery,
            Pickup,
            DeliveryAndPickup,
            Transfer
        }


        public RegionProcessor(Region region) : base(MethodBase.GetCurrentMethod().DeclaringType, region.Identifier)
        {
            _Region = region;
            _ApexConsumer = new ApexConsumer(region, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
            dictCacheFile = Config.DictPath + "Dicts_" + _Region.Identifier + ".json";
            dictCache = new DictCache(dictCacheFile, Logger, region, _ApexConsumer);

        }



        public override void Process()
        {

            if (!MainService.SessionRequired)
            {
                bool errorCaught = false;
                string errorMessage = string.Empty;
                string fatalErrorMessage = string.Empty;


                string successfullRunCacheFile =Config.DictPath + String.Format("{0}-SuccessfulRunTimeCache.json", _Region.Identifier);

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
                if (((DateTime.Now - DateTime.MinValue).TotalHours > Config.DictServiceTimeRefresh) || !File.Exists(dictCacheFile))
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
                    List<ServiceLocation> newServiceLocations = RetrieveNewSLRecords(_Region.Identifier, dictCache.serviceTimeEntityKeyDict, dictCache.timeWindowEntityKeyDict);
                    Logger.InfoFormat("Retrieved {0} New/Updated ServiceLocationsRecords Succesfull", newServiceLocations.Count());

                    if (newServiceLocations != null)
                    {
                        if (newServiceLocations.Count != 0)
                        {

                            List<ServiceLocation> finalizedServiceLocations = prepServiceLocations(newServiceLocations);
                            Logger.InfoFormat("Geocoding {0} New/Updated Service Locations to RNA", newServiceLocations.Count());
                            List<ServiceLocation> geocoded = GeocodeServiceLocations(finalizedServiceLocations);
                            Logger.InfoFormat("Geocoding Complete");
                            Logger.InfoFormat("Saving {0} New/Updated Service Locations to RNA", newServiceLocations.Count());

                            
                            if (geocoded.Count != 0)
                            {
                                SaveSLToRNA(_Region.Identifier, geocoded);
                                dictCache.refreshServiceLocation();
                            }
                        }
                    }
                    Logger.InfoFormat("Service Locations Save Process Finished", newServiceLocations.Count());
                    Logger.InfoFormat("---------------------------------------------------------------------------------");

                    //New Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Start Retrieving Orders from staging table");
                    List<DBAccess.Records.StagedOrderRecord> dbOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
                    List<Order> newDBOrders = RetrieveOrdersSave(_Region.Identifier, out dbOrderRecords, dictCache.orderClassesDict, dictCache.depotsForRegionDict);
                    Logger.InfoFormat("Retrieved {0} Orders from staging table successfully", newDBOrders.Count);
                    Logger.InfoFormat("Start Retrieving Dummy Orders from csv file");
                    List<Order> dummyCSVOrders = RetrieveDummyOrdersSave(_Region.Identifier, dictCache.orderClassesDict, dictCache.depotsForRegionDict);

                    Logger.InfoFormat("Retrieved {0} Dummy Orders Successfully", dummyCSVOrders.Count);
                    Logger.InfoFormat("Seperate Order Types and prepare for saving");
                    List<Order> updateOrders = new List<Order>();
                    List<Order> newOrders = new List<Order>();
                    List<Order> deleteOrders = new List<Order>();
                    List<Order> newDummyOrders = new List<Order>();
                    List<Order> updateDummyOrders = new List<Order>();
                    SeperateOrders(_Region.Identifier, newDBOrders, out updateOrders, out newOrders, out deleteOrders, dummyCSVOrders, out newDummyOrders,
                       out updateDummyOrders, dictCache.serviceLocationDict);
                    Logger.InfoFormat("Seperation and Order Processing Completed");

                    //Update Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Update {0} Orders in RNA", updateOrders.Count);
                    SaveUpdateRNAOrders(_Region.Identifier, updateOrders);
                    Logger.InfoFormat("Update Orders Complete");
                    Logger.InfoFormat("---------------------------------------------------------------------------------");

                    //Update Dummy Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Update {0} Dummy Orders in RNA", updateDummyOrders.Count);
                    SaveDummyUpdateRNAOrders(_Region.Identifier, updateDummyOrders);
                    Logger.InfoFormat("Update Dummy Orders Complete");
                    Logger.InfoFormat("---------------------------------------------------------------------------------");

                    //Delete Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Delete Orders in RNA");
                    DeleteUpdateRNAOrders(_Region.Identifier, deleteOrders);
                    Logger.InfoFormat("Delete Orders Complete");
                    Logger.InfoFormat("---------------------------------------------------------------------------------");


                    //New Dummy Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Add New Dummy Orders to RNA");
                    AddNewDummyOrdersToRNA(newDummyOrders, dictCache.depotsForRegionDict);
                    Logger.InfoFormat("Add New Dummy Orders to RNA Completed");
                    Logger.InfoFormat("---------------------------------------------------------------------------------");

                    //New Order Processing
                    Logger.InfoFormat("---------------------------------------------------------------------------------");
                    Logger.InfoFormat("Add New Orders to RNA");
                    AddNewOrdersToRNA(dbOrderRecords.Where(order => newOrders.Any(newRnaOrder => newRnaOrder.Identifier == order.OrderIdentifier)).ToList(), newOrders);
                    Logger.InfoFormat("Add New Orders to RNA Completed");
                    Logger.InfoFormat("---------------------------------------------------------------------------------");



                    //Orders Processing * correct Save Order Result

                    //_ApexConsumer.RetrieveOrdersandSaveToRNA(dictCache.regionEntityKeyDict, dictCache.depotsForRegionDict, dictCache.orderClassesDict,
                    //   _Region.Identifier, out errorCaught, out errorMessage, out fatalErrorMessage, out timeOut);



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

        private List<ServiceLocation> RetrieveNewSLRecords(string regionID, Dictionary<string, long> serviceTimeCache, Dictionary<string, long> timeWindowCache)
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

                    foreach (DBAccess.Records.StagedServiceLocationRecord locationRecord in retrieveNewSLRecords)
                    {
                        try
                        {
                            if (!errorSLRecords.Contains(locationRecord))
                            {
                                ServiceLocation location = (ServiceLocation)locationRecord;
                                location.ServiceTimeTypeEntityKey = serviceTimeCache[locationRecord.ServiceTimeTypeIdentifier];
                                location.TimeWindowTypeEntityKey = timeWindowCache[locationRecord.ServiceWindowTypeIdentifier];
                                newServiceLocations.Add(location);
                            }
                        }
                        catch (Exception ex)
                        {
                           
                            errorSLMessage = ex.Message;
                            Logger.Error("Error Retrieveing New SL's from Database: " + errorSLMessage);
                            _IntegrationDBAccessor.UpdateServiceLocationStatus(locationRecord.RegionIdentifier, locationRecord.ServiceLocationIdentifier, errorSLMessage, "ERROR", out errorSLMessage, out errorCaught);
                            if (errorCaught)
                            {
                                Logger.Error("Error Updating SL " + locationRecord.ServiceLocationIdentifier + " with Error Status | " + errorSLMessage);

                            }
                            else
                            {
                                Logger.Debug("Service Location " + locationRecord.ServiceLocationIdentifier + " error status update successfully");
                            }
                        }

                    }


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

                        _IntegrationDBAccessor.UpdateServiceLocationStatus(sLrecord.RegionIdentifier, sLrecord.ServiceLocationIdentifier, errorMessage, "ERROR", out errorSLMessage, out errorCaught);
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


        private List<ServiceLocation> prepServiceLocations(List<ServiceLocation> serviceLocations)
        {
            List<ServiceLocation> updateServiceLocations = new List<ServiceLocation>();
            string[] serviceLocationIdentifiers = serviceLocations.Select(sl => sl.Identifier).ToArray();


            bool errorCaught = false;
            string errorSLMessage = string.Empty;
            try
            {
                updateServiceLocations = _ApexConsumer.RetrieveServiceLocations(out errorCaught, out errorSLMessage, serviceLocationIdentifiers).ToList();
                if (!errorCaught)
                {
                    serviceLocations.Where(sl => updateServiceLocations.Any(x => x.Identifier == sl.Identifier)).ToList()
                        .ForEach(x => { x.Action = ActionType.Update; x.EntityKey = updateServiceLocations.Find(sl => serviceLocations.Any(z => z.Identifier == sl.Identifier)).EntityKey; });

                    Logger.InfoFormat("Start Geocoding {0} Service Location", serviceLocations.Count());
                    serviceLocations = GeoServiceLocations(serviceLocations);

                    return serviceLocations;
                }
                else
                {
                    Logger.Error("Error Checking RNA for SL's : " + errorSLMessage);
                    return serviceLocations;
                }

            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorSLMessage = ex.Message;
                Logger.Error("Error Checking RNA for SL's : " + errorSLMessage);
            }
            return updateServiceLocations;
        }

        private List<ServiceLocation> GeocodeServiceLocations(List<ServiceLocation> serviceLocations)
        {
            List<ServiceLocation> geocodeServiceLocations = new List<ServiceLocation>();
            string[] serviceLocationIdentifiers = serviceLocations.Select(sl => sl.Identifier).ToArray();


            bool errorCaught = false;
          
            bool timeout = false;
            string errorSLMessage = string.Empty;

            foreach (ServiceLocation location in serviceLocations)
            {
                try
                {


                    GeocodeCandidate candidate = _ApexConsumer.GeocodeRNA(out errorCaught, out errorSLMessage, out timeout,new Address[]{ location.Address });
                    if (!errorCaught && !timeout)
                    {
                        location.Coordinate = candidate.Coordinate;
                        location.GeocodeAccuracy_GeocodeAccuracy = candidate.GeocodeAccuracy_Quality;
                        Logger.InfoFormat("Start Geocoding {0} Service Location", serviceLocations.Count());
                        serviceLocations = GeoServiceLocations(serviceLocations);

                        geocodeServiceLocations.Add(location);
                    }
                    else if (errorCaught)
                    {
                        bool errorSave = false;
                        string errorMessage = string.Empty;
                        Logger.ErrorFormat("Error Geocoding Service Location {0} "+ errorSLMessage, location.Identifier);
                        _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, errorSLMessage + " Will still Attempt to Save See Log", "", out errorMessage, out errorSave);
                        if (errorCaught)
                        {
                            Logger.Debug("Updating Service Location " + location.Identifier + " error status in staging table failed | " + errorMessage);

                        }
                        else
                        {
                            Logger.Debug("Updating Service Location " + location.Identifier + " error status succesfull");
                        }
                        geocodeServiceLocations.Add(location);

                    } else if (timeout)
                    {
                        bool errorSave = false;
                        string errorMessage = string.Empty;
                        Logger.ErrorFormat("Error Geocoding Service Location {0}. Geocoding Timed  Out" , location.Identifier);
                        _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, "Error Geocoding ServiceLocation. Operation TimedOut" + "Will still Attempt to Save See Log", "", out errorMessage, out errorSave);
                        if (errorCaught)
                        {
                            Logger.Debug("Updating Service Location " + location.Identifier + " error status in staging table failed | " + errorMessage);

                        }
                        else
                        {
                            Logger.Debug("Updating Service Location " + location.Identifier + " error status succesfull");
                        }
                        geocodeServiceLocations.Add(location);
                    }
                }
                catch (Exception ex)
                {
                    errorCaught = true;
                    errorSLMessage = ex.Message;
                    Logger.Error("Error Checking RNA for SL's : " + errorSLMessage);
                    _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, errorSLMessage + "See Log", "ERROR", out errorSLMessage, out errorCaught);
                    if (errorCaught)
                    {
                        Logger.Debug("Updating Service Location " + location.Identifier + " error status in staging table failed | " + errorSLMessage);
                        geocodeServiceLocations.Add(location);
                    }
                    else
                    {
                        Logger.Debug("Updating Service Location " + location.Identifier + " error status succesfull");
                        geocodeServiceLocations.Add(location);
                    }
                }
            }
            
            return geocodeServiceLocations;
        }

        private void SaveSLToRNA(string regionID, List<ServiceLocation> serviceLocations)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            try
            {
                foreach (ServiceLocation location in serviceLocations)
                {
                    SaveResult saveResult = _ApexConsumer.SaveRNAServiceLocations(out errorCaught, out errorMessage, new ServiceLocation[] { location });
                    if (!errorCaught)
                    {
                        if (saveResult != null)
                        {


                            errorCaught = false;
                            errorMessage = string.Empty;
                            if (saveResult.Error != null)
                            {

                                if (saveResult.Error.Code.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {
                                    string message = string.Empty;
                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {
                                        message = message + " | " + string.Format(" ErrorCode: ValidationError ErrorProperty: " + validFailure.Property + " ErrorDetail: " + validFailure.FailureType_Type + "\r\n");
                                        Logger.Debug("A Validation Error Occured While Saving Service Locations. The " + validFailure.Property + " Property for Order " + location.Identifier + " is not Valid");
                                        Logger.Debug("Updating Service Location " + location.Identifier + " db record status to Error");

                                    }
                                    _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, message + "See Log", "ERROR", out errorMessage, out errorCaught);
                                    if (errorCaught)
                                    {
                                        Logger.Debug("Updating Service Location " + location.Identifier + " error status in staging table failed | " + errorMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Service Location " + location.Identifier + " error status succesfull");
                                    }
                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Saving Service Locations. The " + saveResult.Error.Code.ErrorCode_Status + " Order " + location.Identifier + " is not Valid");
                                    Logger.Debug("Updating Service Location " + location.Identifier + " db records status to Error");
                                    _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, "Error: " + saveResult.Error.Code.ErrorCode_Status + " See Log", "ERROR", out errorMessage, out errorCaught);
                                    if (errorCaught)
                                    {
                                        Logger.Error("Updating Service Location " + location.Identifier + " error status in staging table failed | " + errorMessage);

                                    }
                                    else
                                    {
                                        Logger.Info("Updating Order " + location.Identifier + " error status succesfull");
                                    }

                                }
                            }
                            else
                            {
                                Logger.Debug("Saving/Updating Order : " + location.Identifier + " to RNA Successfull ");
                                _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, "", "COMPLETE", out errorMessage, out errorCaught);
                                if (errorCaught)
                                {
                                    Logger.Error("Updating Service Location " + location.Identifier + " status in staging table failed | " + errorMessage);

                                }
                                else
                                {
                                    Logger.Info("Updating Order " + location.Identifier + " status succesfull");
                                }
                            }



                        }

                    }
                    else
                    {
                        Logger.Error("Error Saving Service Location " + location.Identifier + " | " + errorMessage);
                        _IntegrationDBAccessor.UpdateServiceLocationStatus(_Region.Identifier, location.Identifier, "ERROR: " + errorMessage, "ERROR", out errorMessage, out errorCaught);
                        if (errorCaught)
                        {
                            Logger.Debug("Updating Service Location " + location.Identifier + " error status in staging table failed | " + errorMessage);

                        }
                        else
                        {
                            Logger.Debug("Updating Order " + location.Identifier + " error status succesfull");
                        }
                    }
                }

            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }

        }

        private void SeperateOrders(string regionID, List<Order> orders, out List<Order> updateOrders,
            out List<Order> newOrders, out List<Order> deleteOrders, List<Order> dummyOrders, out List<Order> newDummyOrders,
            out List<Order> updateDummyOrders, Dictionary<string, ServiceLocation> serviceLocationsDict)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            List<OrderSpec> orderSpecs = new List<OrderSpec>();

            List<string> orderIdentifiers = orders.Select(order => order.Identifier).ToList();
            List<string> dummyOrderIdentifiers = dummyOrders.Select(order => order.Identifier).ToList();
            List<Order> errorDeletOrders = new List<Order>();
            List<Order> potentialNewOrder = new List<Order>();
            newOrders = new List<Order>();
            updateOrders = new List<Order>();
            deleteOrders = new List<Order>();
            newDummyOrders = new List<Order>();
            updateDummyOrders = new List<Order>();

            try
            {
                List<Order> ordersFromRNA = _ApexConsumer.RetrieveOrdersFromRNA(out errorCaught, out errorMessage, orderIdentifiers.ToArray());

                if (!errorCaught)
                {
                    updateOrders = ordersFromRNA.FindAll(order => orders.Any(rnOrder => rnOrder.Identifier == order.Identifier && rnOrder.Action != ActionType.Delete) && orders.
                    Any(rnOrder => rnOrder.BeginDate == order.BeginDate)).ToList();
                    potentialNewOrder = orders.FindAll(order => order.Action != ActionType.Delete && !ordersFromRNA.Any(rnOrder => rnOrder.Identifier == order.Identifier &&
                    rnOrder.BeginDate == order.BeginDate)).ToList();
                    potentialNewOrder.ForEach(x =>
                    {
                        x.Action = ActionType.Add; x.ManagedByUserEntityKey = MainService.User.EntityKey;
                        x.CreatedBy = MainService.User.EmailAddress; x.RegionEntityKey = _Region.EntityKey;
                    });

                    List<Order> tempOrder = new List<Order>();
                    
                    deleteOrders = ordersFromRNA.FindAll(order => orders.Any(rnOrder => rnOrder.Identifier == order.Identifier && rnOrder.Action == ActionType.Delete));
                    tempOrder = deleteOrders;
                    errorDeletOrders = orders.FindAll(errorOrder => !tempOrder.Any(order => errorOrder.Identifier == order.Identifier) && errorOrder.Action == ActionType.Delete);
                }
                else
                {
                    Logger.Error("Error Caught Checking if Orders Exist in RNA : " + errorMessage);
                }
                foreach(Order deleteORder in errorDeletOrders)
                {
                    bool temperrorCaught = false;
                    string temperrorMessage = string.Empty;
                    Logger.ErrorFormat("Error seperating delete order {0} order not found in RNA", deleteORder.Identifier);
                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, deleteORder.Identifier, "Delete Order not Found in RNA " + "See Log", "ERROR", out temperrorMessage, out temperrorCaught);
                    if (temperrorCaught)
                    {
                        Logger.Debug("Updating Order " + deleteORder.Identifier + " error status in staging table failed | " + temperrorMessage);

                    }
                    else
                    {
                        Logger.Debug("Updating Order " + deleteORder.Identifier + " error status succesfull");
                    }
                }

                foreach (Order updateOrder in updateOrders)
                {

                    Order databaseOrder = orders.Find(order => (order.Identifier == updateOrder.Identifier) &&
                    (order.BeginDate == updateOrder.BeginDate));
                    updateOrder.Action = ActionType.Update;
                    updateOrder.Tasks[0].ServiceWindowOverrides = ServiceWindowConsolidation(updateOrder, databaseOrder);
                    updateOrder.ManagedByUserEntityKey = MainService.User.EntityKey;
                    updateOrder.Tasks[0].LocationEntityKey = serviceLocationsDict[databaseOrder.Tasks[0].LocationIdentifier].EntityKey;
                    updateOrder.PlannedDeliveryQuantities.Size1 = databaseOrder.PlannedDeliveryQuantities.Size1;
                    updateOrder.PlannedDeliveryQuantities.Size2 = databaseOrder.PlannedDeliveryQuantities.Size2;
                    updateOrder.PlannedDeliveryQuantities.Size3 = databaseOrder.PlannedDeliveryQuantities.Size3;
                    updateOrder.SpecialInstructions = databaseOrder.SpecialInstructions;
                    updateOrder.CustomProperties = databaseOrder.CustomProperties;


                }
                if (newOrders == null) { newOrders = new List<Order>(); }
                if (deleteOrders == null) { deleteOrders = new List<Order>(); }
                foreach (Order deleteOrder in deleteOrders)
                {
                    Order databaseOrder = orders.Find(order => (order.Identifier == deleteOrder.Identifier) &&
                    (order.BeginDate == deleteOrder.BeginDate));
                    deleteOrder.EntityKey = ordersFromRNA.Find(order => order.Identifier == databaseOrder.Identifier).EntityKey;
                    deleteOrder.ManagedByUserEntityKey = MainService.User.EntityKey;
                    deleteOrder.RegionEntityKey = _Region.EntityKey;

                }
                foreach (Order newOrder in potentialNewOrder)
                {
                    ServiceLocation location = new ServiceLocation();
                    Order databaseOrder = orders.Find(order => (order.Identifier == newOrder.Identifier) &&
                       (order.BeginDate == newOrder.BeginDate));
                    bool locationEKFound = serviceLocationsDict.TryGetValue(databaseOrder.Tasks[0].LocationIdentifier, out location);
                    if(locationEKFound)
                    {
                        newOrder.Tasks[0].LocationEntityKey = serviceLocationsDict[databaseOrder.Tasks[0].LocationIdentifier].EntityKey;
                        newOrder.LineItems = new LineItem[] { };
                        newOrders.Add(newOrder);
                    }
                    else
                    {
                      
                        bool errorSave = false;
                        string errorSaveMessage = string.Empty;
                        string message = String.Format("Order {0} | ServiceLocation {1} not found in RNA", databaseOrder.Identifier, databaseOrder.Tasks[0].LocationIdentifier);
                        Logger.Error(message);
                        _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, databaseOrder.Identifier, message + "See Log", "ERROR", out errorSaveMessage, out errorSave);
                        if (errorSave)
                        {
                            Logger.Debug("Updating Order " + databaseOrder.Identifier + " error status in staging table failed | " + errorSaveMessage);

                        }
                        else
                        {
                            Logger.Debug("Updating Order " + databaseOrder.Identifier + " error status succesfull");
                        }
                    }

                       
                     
                    


                }


                List<Order> dummOrdersFromRNA = _ApexConsumer.RetrieveDummyRNAOrders(out errorCaught, out errorMessage, dummyOrderIdentifiers.ToArray());
                List<Order> tempNewDummyOrders = new List<Order>();
                if (!errorCaught)
                {
                    updateDummyOrders = dummOrdersFromRNA.Where(order => dummyOrders.Any(rnOrder => rnOrder.Identifier == order.Identifier && rnOrder.Action != ActionType.Delete) && dummyOrders.
                    Any(rnOrder => rnOrder.BeginDate == order.BeginDate)).ToList();
                    tempNewDummyOrders = dummyOrders.FindAll(order => order.Action != ActionType.Delete && !dummOrdersFromRNA.Any(rnOrder => rnOrder.Identifier == order.Identifier)).ToList();
                    tempNewDummyOrders.ForEach(x => { x.Action = ActionType.Add; x.ManagedByUserEntityKey = MainService.User.EntityKey; x.RegionEntityKey = _Region.EntityKey; });
                }
                else
                {
                    Logger.Error("Error Caught Checking if Dummy Orders Exist in RNA : " + errorMessage);
                }

                foreach (Order newDummyOrder in tempNewDummyOrders)
                {
                    Order csvDummyOrder = newDummyOrders.Find(order => (order.Identifier == newDummyOrder.Identifier));
                    newDummyOrder.Tasks[0].LocationEntityKey = serviceLocationsDict[csvDummyOrder.Tasks[0].LocationIdentifier].EntityKey;
                    ServiceLocation location = new ServiceLocation();
                    bool locationEKFound = serviceLocationsDict.TryGetValue(csvDummyOrder.Tasks[0].LocationIdentifier, out location);
                    if (locationEKFound)
                    {
                        newDummyOrder.Tasks[0].LocationEntityKey = serviceLocationsDict[csvDummyOrder.Tasks[0].LocationIdentifier].EntityKey;

                        newDummyOrders.Add(newDummyOrder);
                    }
                    else
                    {

                       
                        string message = String.Format("Order {0} | ServiceLocation {1} not found in RNA", csvDummyOrder.Identifier, csvDummyOrder.Tasks[0].LocationIdentifier);
                        Logger.Error(message);
                       
                    }
                }

                foreach (Order updateDummyOrder in updateDummyOrders)
                {

                    Order csvOrder = dummyOrders.Find(order => (order.Identifier == updateDummyOrder.Identifier));
                    updateDummyOrder.Action = ActionType.Update;
                    updateDummyOrder.Tasks[0].ServiceWindowOverrides = ServiceWindowConsolidation(updateDummyOrder, csvOrder);
                    updateDummyOrder.ManagedByUserEntityKey = MainService.User.EntityKey;
                    updateDummyOrder.PlannedPickupQuantities.Size1 = csvOrder.PlannedPickupQuantities.Size1;
                    updateDummyOrder.PlannedPickupQuantities.Size2 = csvOrder.PlannedPickupQuantities.Size2;
                    updateDummyOrder.PlannedPickupQuantities.Size3 = csvOrder.PlannedPickupQuantities.Size3;
                  
                  
                   

                }

            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving/Updating/Deleting Service Locations into RNA: " + errorMessage);
            }



        }

        private void SaveUpdateRNAOrders(string regionID, List<Order> orders)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            System.Collections.Concurrent.ConcurrentBag<OrderSpec> orderSpecs = new System.Collections.Concurrent.ConcurrentBag<OrderSpec>();

            System.Threading.Tasks.Parallel.ForEach(orders, (rnaOrder) =>
            {
                orderSpecs.Add(ConvertOrderToOrderSpec(rnaOrder));

            });


            try
            {
                foreach (OrderSpec updateOrderSpec in orderSpecs)
                {


                    List<SaveResult> saveOrdersResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { updateOrderSpec }).ToList();
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
                                    string message = string.Empty;
                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {
                                        message = string.Format(" | " + message + " ErrorCode: ValidationError, ErrorDetail: {0}, ErrorProperty: {1} \r\n", validFailure.FailureType_Type, validFailure.Property);
                                        Logger.Debug("A Validation Error Occured While Saving Orders. The " + validFailure.Property + " Property for Order " + updateOrderSpec.Identifier + " is not Valid");
                                        Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " db record status to Error");

                                    }
                                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, updateOrderSpec.Identifier, message + "See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                    if (errorUpdatingServiceLocation)
                                    {
                                        Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " error status succesfull");
                                    }
                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Saving Orders. The " + saveResult.Error.Code.ErrorCode_Status + " Order " + updateOrderSpec.Identifier + " is not Valid");
                                    Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " db records status to Error");
                                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, updateOrderSpec.Identifier, "Error: " + saveResult.Error.Code.ErrorCode_Status + " See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                    if (errorUpdatingServiceLocation)
                                    {
                                        Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " error status succesfull");
                                    }

                                }
                            }

                        }
                        foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error == null)).ToList())
                        {
                            Logger.Debug("Saving/Updating Order : " + updateOrderSpec.Identifier + " to RNA Successfull ");
                            _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, updateOrderSpec.Identifier, "", "COMPLETE", out errorMessage, out errorCaught);
                            if (errorCaught)
                            {
                                Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " error status in staging table failed | " + errorMessage);

                            }
                            else
                            {
                                Logger.Debug("Updating Order " + updateOrderSpec.Identifier + " error status succesfull");
                            }
                        }

                    }
                    else
                    {
                        Logger.Error("Error Caught Saving Orders to RNA : " + errorMessage);

                    }

                }
            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error(" Error Updating Order into RNA: " + errorMessage);
            }



        }

        private void SaveNewDummyRNAOrders(string regionID, List<Order> orders, Dictionary<string, long> depotCache)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            System.Collections.Concurrent.ConcurrentBag<OrderSpec> orderSpecs = new System.Collections.Concurrent.ConcurrentBag<OrderSpec>();

            System.Threading.Tasks.Parallel.ForEach(orders, (rnaOrder) =>
            {
                OrderSpec temp = ConvertOrderToOrderSpec(rnaOrder);
                temp.OrderInstance = null;
                orderSpecs.Add(temp);
               
            });


            try
            {
                foreach (OrderSpec updateOrderSpec in orderSpecs)
                {
                    PickupTaskSpec pickUp = (PickupTaskSpec)updateOrderSpec.TaskSpec;

                    string depotIdentifier = depotCache.FirstOrDefault(x => x.Value == pickUp.RequiredDestinationEntityKey).Key;
                    DateTime sessionDate = DateTime.Parse(updateOrderSpec.BeginDate);
                    DailyRoutingSession dailyRoutingSession = _ApexConsumer.RetrieveDailyRoutingSessionwithOrigin(out errorCaught, out errorMessage, sessionDate, depotIdentifier).DefaultIfEmpty(null).FirstOrDefault();

                    List<SaveResult> saveOrdersResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { updateOrderSpec }).ToList();
                    if (!errorCaught)
                    {
                        foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error != null)).ToList())
                        {
                            var tempOrder = (Order)saveResult.Object;

                            string errorUpdatingServiceLocationMessage = string.Empty;
                            if (saveResult.Error != null)
                            {


                                if (saveResult.Error.Code.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {
                                    string message = string.Empty;
                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {

                                        Logger.Debug("A Validation Error Occured While Saving Orders. The " + validFailure.Property + " Property for Order " + updateOrderSpec.Identifier + " is not Valid");
                                        Logger.Debug("Updating Dummy Order " + updateOrderSpec.Identifier + " db record status to Error");

                                    }

                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Saving  DummyOrders. The " + saveResult.Error.Code.ErrorCode_Status + " Dummy Order " + updateOrderSpec.Identifier + " is not Valid");
                                    Logger.Debug("Updating Dummy Order " + updateOrderSpec.Identifier + " db records status to Error");


                                }
                            }

                        }
                        foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error == null)).ToList())
                        {
                            Logger.Debug("Saving/Updating Dummy Order : " + updateOrderSpec.Identifier + " to RNA Successfull ");

                        }

                    }
                    else
                    {
                        Logger.Error("Error Caught Saving Dummy Orders to RNA : " + errorMessage);

                    }

                }
            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error(" Error Updating Dummy Order into RNA: " + errorMessage);
            }



        }

        private void SaveDummyUpdateRNAOrders(string regionID, List<Order> orders)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            System.Collections.Concurrent.ConcurrentBag<OrderSpec> orderSpecs = new System.Collections.Concurrent.ConcurrentBag<OrderSpec>();

            System.Threading.Tasks.Parallel.ForEach(orders, (rnaOrder) =>
            {
                orderSpecs.Add(ConvertOrderToOrderSpec(rnaOrder));

            });


            try
            {
                foreach (OrderSpec updateOrderSpec in orderSpecs)
                {


                    List<SaveResult> saveOrdersResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { updateOrderSpec }).ToList();
                    if (!errorCaught)
                    {
                        foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error != null)).ToList())
                        {
                            var tempOrder = (Order)saveResult.Object;

                            string errorUpdatingServiceLocationMessage = string.Empty;
                            if (saveResult.Error != null)
                            {


                                if (saveResult.Error.Code.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {
                                    string message = string.Empty;
                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {

                                        Logger.Debug("A Validation Error Occured While Saving Orders. The " + validFailure.Property + " Property for Order " + updateOrderSpec.Identifier + " is not Valid");
                                        Logger.Debug("Updating Dummy Order " + updateOrderSpec.Identifier + " db record status to Error");

                                    }

                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Saving  DummyOrders. The " + saveResult.Error.Code.ErrorCode_Status + " Dummy Order " + updateOrderSpec.Identifier + " is not Valid");
                                    Logger.Debug("Updating Dummy Order " + updateOrderSpec.Identifier + " db records status to Error");


                                }
                            }

                        }
                        foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error == null)).ToList())
                        {
                            Logger.Debug("Saving/Updating Dummy Order : " + updateOrderSpec.Identifier + " to RNA Successfull ");

                        }

                    }
                    else
                    {
                        Logger.Error("Error Caught Saving Dummy Orders to RNA : " + errorMessage);

                    }

                }
            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error(" Error Updating Dummy Order into RNA: " + errorMessage);
            }



        }

        private void SaveNewDummyOrdersToRoutes(string regionID, List<Order> orders)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            System.Collections.Concurrent.ConcurrentBag<OrderSpec> orderSpecs = new System.Collections.Concurrent.ConcurrentBag<OrderSpec>();

            System.Threading.Tasks.Parallel.ForEach(orders, (rnaOrder) =>
            {
                orderSpecs.Add(ConvertOrderToOrderSpec(rnaOrder));

            });


            try
            {
                foreach (OrderSpec updateOrderSpec in orderSpecs)
                {


                    List<SaveResult> saveOrdersResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { updateOrderSpec }).ToList();
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
                                    string message = string.Empty;
                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {

                                        Logger.Debug("A Validation Error Occured While Saving Dummy Orders. The " + validFailure.Property + " Property for Dummy Order " + updateOrderSpec.Identifier + " is not Valid");


                                    }

                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Saving Dummy Orders. The " + saveResult.Error.Code.ErrorCode_Status + "Dummy Order " + updateOrderSpec.Identifier + " is not Valid");



                                }
                            }

                        }
                        foreach (SaveResult saveResult in saveOrdersResult.Where(result => (result.Error == null)).ToList())
                        {
                            Logger.Debug("Saving Dummy Order : " + updateOrderSpec.Identifier + " to RNA Successfull ");

                        }

                    }
                    else
                    {
                        Logger.Error("Error Caught Saving Dummy Orders to RNA : " + errorMessage);

                    }

                }
            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Saving Dummy Orders into RNA: " + errorMessage);
            }



        }

        private List<Order> RetrieveOrdersSave(string regionID, out List<DBAccess.Records.StagedOrderRecord> stagedOrderRecords,
            Dictionary<string, long> orderClassCache, Dictionary<string, long> depotCache)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            List<Order> newOrders = new List<Order>();
            stagedOrderRecords = new List<DBAccess.Records.StagedOrderRecord>();
            newOrders = RetrieveNewOrderRecords(out errorCaught, out errorMessage, out stagedOrderRecords, regionID, orderClassCache, depotCache);
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


        private List<Order> RetrieveDummyOrdersSave(string regionID, Dictionary<string, long> orderClassCache, Dictionary<string, long> depotCache)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            List<Order> newOrders = new List<Order>();

            newOrders = _ApexConsumer.RetrieveDummyOrdersFromCSV(depotCache, orderClassCache, regionID, out errorCaught, out errorMessage);
            if (!errorCaught)
            {

                if (newOrders.Count == 0)
                {

                    Logger.Info("No Dummy orders Found csv file");
                    return new List<Order>();

                }
                else
                {
                    return newOrders;

                }

            }
            else
            {
                Logger.Error("Error Ocurred Retrieving dummy Orders from CSV file " + errorMessage);
                return new List<Order>();

            }


        }

        private List<Order> RetrieveNewOrderRecords(out bool errorRetrieveOrdersFromTable, out string errorRetrieveOrdersFromTableMessage, out List<DBAccess.Records.StagedOrderRecord> stagedOrderRecords, string regionID,
             Dictionary<string, long> orderClassCache, Dictionary<string, long> depotCache)
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

                    List<DBAccess.Records.StagedOrderRecord> errorOrderRecords = retrieveOrderRecords.Where(orderRecord =>
                    (orderRecord.OrderIdentifier == null || orderRecord.OrderIdentifier.Length == 0) ||
                  (orderRecord.BeginDate == null || orderRecord.BeginDate.Length == 0) ||
                  (orderRecord.RegionIdentifier == null || orderRecord.RegionIdentifier.Length == 0) ||
                  (orderRecord.OrderClassIdentifier == null || orderRecord.OrderClassIdentifier.Length == 0) ||
                  (orderRecord.QuantitySize1 == null || orderRecord.QuantitySize1.Length == 0) ||
                  (orderRecord.QuantitySize2 == null || orderRecord.QuantitySize2.Length == 0) ||
                  (orderRecord.QuantitySize3 == null || orderRecord.QuantitySize3.Length == 0) ||
                  (orderRecord.PreferredRouteIdentifier == null || orderRecord.PreferredRouteIdentifier.Length == 0) ||
                  (orderRecord.OriginDepotIdentifier == null || orderRecord.OriginDepotIdentifier.Length == 0) ||
                   (!depotCache.ContainsKey(orderRecord.OriginDepotIdentifier)) ||
                   (!orderClassCache.ContainsKey(orderRecord.OrderClassIdentifier))).ToList();

                    retrieveOrderRecords.RemoveAll(orderRecord => errorOrderRecords.Exists(y => y == orderRecord));
                    stagedOrderRecords = retrieveOrderRecords;
                    foreach (DBAccess.Records.StagedOrderRecord orderRecord in stagedOrderRecords)
                    {
                        Order newOrder = (Order)orderRecord;
                        newOrder.RequiredRouteOriginEntityKey = depotCache[orderRecord.OriginDepotIdentifier];
                        newOrder.OrderClassEntityKey = orderClassCache[orderRecord.OrderClassIdentifier];
                        newOrders.Add(newOrder);
                    }

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

                        }
                        if (orderRecord.PreferredRouteIdentifier == null || orderRecord.PreferredRouteIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "PreferredRouteIdentifier is null or empty | ";
                        }
                        if (orderRecord.OriginDepotIdentifier == null || orderRecord.OriginDepotIdentifier.Length == 0)
                        {
                            errorMessage = errorMessage + "OriginDepotIdentifier is null or empty | ";
                        }
                        if (!depotCache.ContainsKey(orderRecord.OriginDepotIdentifier))
                        {
                            errorMessage = errorMessage + "OriginDepotIdentifier not found in RNA | ";
                        }

                        if (!orderClassCache.ContainsKey(orderRecord.OrderClassIdentifier))
                        {
                            errorMessage = errorMessage + "OrderClass not found in RNA | ";
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

        private List<ServiceLocation> GeoServiceLocations(List<ServiceLocation> serviceLocations)
        {
            System.Collections.Concurrent.ConcurrentBag<ServiceLocation> serviceLocationBag = new System.Collections.Concurrent.ConcurrentBag<ServiceLocation>(serviceLocations);

            foreach(ServiceLocation serviceLocation in serviceLocationBag)
            {
                bool errorCaught = false;
                bool timeout = false;
                string errorMessage = string.Empty;

                Logger.DebugFormat("Start Geocoding Service Locations {0}", serviceLocation.Identifier);
                GeocodeCandidate result = _ApexConsumer.GeocodeRNA(out errorCaught, out errorMessage, out timeout, new Address[] { serviceLocation.Address });
                if (!errorCaught)
                {
                    serviceLocation.Coordinate = result.Coordinate;
                    serviceLocation.GeocodeAccuracy_GeocodeAccuracy = result.GeocodeAccuracy_Quality;

                    Logger.ErrorFormat("Geocoding Service Locations {0} Successful ", serviceLocation.Identifier);
                }
                else
                {
                    Logger.ErrorFormat("Error Geocoding Service Locations {0} : " + errorMessage, serviceLocation.Identifier);
                }


            }




            return serviceLocationBag.ToList();

        }

        private void AddNewOrdersToRNA(List<DBAccess.Records.StagedOrderRecord> newOrderRecords, List<Order> newOrders)
        {
            List<Order> preppedOrder = new List<Order>();
            bool errorCaught = false;
            string errorMessage = string.Empty;
            System.Collections.Concurrent.ConcurrentBag<Order> newOrdersBag = new System.Collections.Concurrent.ConcurrentBag<Order>(newOrders);

            foreach(DBAccess.Records.StagedOrderRecord order in newOrderRecords)
            {
                Order newRNAOrder = newOrdersBag.Where(x => x.Identifier == order.OrderIdentifier).First();
                DateTime sessionDate = Convert.ToDateTime(order.BeginDate);
                if (order.PreferredRouteIdentifier != string.Empty || order.PreferredRouteIdentifier != null)
                {

                    Route route = _ApexConsumer.RetrieveRoute(out errorCaught, out errorMessage, order.PreferredRouteIdentifier, sessionDate, order.OriginDepotIdentifier);
                   
                   

                    if (!errorCaught)
                    {
                        if (route != null) // route found, assign order to route
                        {

                            if (newOrdersBag.Any(x => x.Identifier == newRNAOrder.Identifier && x.BeginDate == newRNAOrder.BeginDate))
                            {
                                newRNAOrder = newOrdersBag.Where(x => x.Identifier == newRNAOrder.Identifier).First();
                                newRNAOrder.SessionEntityKey = route.RoutingSessionEntityKey;
                                newRNAOrder.SessionDescription = route.RoutingSessionDescription;
                                newRNAOrder.SessionDate = route.RoutingSessionDate;
                            }

                            bool assignedOrderSuccess = AssignOrderToRoute(newRNAOrder, route.EntityKey, route.Version);
                            if (assignedOrderSuccess)
                            {
                                Logger.InfoFormat("Order {0} assigned to route {1} successfully", newRNAOrder.Identifier, newRNAOrder.PreferredRouteIdentifier);
                            }
                            else
                            {
                                Logger.ErrorFormat("Error Order {0} assigned to route {1}. See logs and table", newRNAOrder.Identifier, newRNAOrder.PreferredRouteIdentifier);
                            }

                        }
                        else // Route not found, see if routing session Exist, if not create routing session and route, if it does just create route
                        {
                            DailyRoutingSession dailyRoutingSession = _ApexConsumer.RetrieveDailyRoutingSessionwithOrigin(out errorCaught, out errorMessage, sessionDate, order.OriginDepotIdentifier).DefaultIfEmpty(null).FirstOrDefault();

                            if (dailyRoutingSession != null) // just create route and update order with session information
                            {
                                newRNAOrder = newOrdersBag.Where(x => x.Identifier == newRNAOrder.Identifier).First();
                                newRNAOrder.SessionEntityKey = dailyRoutingSession.EntityKey;
                                newRNAOrder.SessionDescription = dailyRoutingSession.Description;
                                newRNAOrder.SessionDate = dailyRoutingSession.StartDate;
                                long? passEntityKey = GetorCreateRoutingSessionPass(dailyRoutingSession, newRNAOrder);
                                if (passEntityKey.HasValue)
                                {
                                    long? rtEntityKey = CreateRoute(order.PreferredRouteIdentifier, sessionDate, (long)passEntityKey, newRNAOrder);

                                    if (rtEntityKey.HasValue)
                                    {
                                        bool assignedOrderSuccess = AssignOrderToRoute(newRNAOrder, (long)rtEntityKey, 1);
                                        if (assignedOrderSuccess)
                                        {
                                            Logger.InfoFormat("Order {0} assigned to route {1} successfully", newRNAOrder.Identifier, newRNAOrder.PreferredRouteIdentifier);
                                        } else
                                        {
                                            Logger.ErrorFormat("Error Order {0} assigned to route {1}. See logs and table", newRNAOrder.Identifier, newRNAOrder.PreferredRouteIdentifier);
                                        }

                                    }




                                }
                                else
                                {
                                    Logger.ErrorFormat("Daily Routing Pass for routing session {0} on date {1}, not created, cannot create Route", dailyRoutingSession.Description, dailyRoutingSession.StartDate);
                                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, newRNAOrder.Identifier, "Error assigning Order to Route: No Daily Routing Pass Found for RoutingSession , See Log", "ERROR", out errorMessage, out errorCaught);
                                    if (errorCaught)
                                    {
                                        Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", newRNAOrder.Identifier, errorMessage);
                                    }
                                    else
                                    {
                                        Logger.InfoFormat(" Updating Order Table for order {0} Successful ", newRNAOrder.Identifier, errorMessage);
                                    }
                                }



                            }
                            else// session is null create new routing session, create new route, update order with session information;
                            {
                                
                                SaveResult[] dsSaveResult = _ApexConsumer.SaveDailyRoutingSessions(out errorCaught, out errorMessage, new DateTime[] { sessionDate }, new string[] { order.OriginDepotIdentifier }, newRNAOrder);
                                if (dsSaveResult[0].Error == null)
                                {
                                    DailyRoutingSession rtSession = (DailyRoutingSession)dsSaveResult[0].Object;
                                    newRNAOrder = newOrdersBag.Where(x => x.Identifier == newRNAOrder.Identifier).First();
                                    newRNAOrder.SessionEntityKey = rtSession.EntityKey;
                                    newRNAOrder.SessionDescription = rtSession.Description;
                                    newRNAOrder.SessionDate = rtSession.StartDate;
                                    long? passEntityKey = GetorCreateRoutingSessionPass(rtSession, newRNAOrder);
                                    if (passEntityKey.HasValue)
                                    {
                                        long? rtEntityKey = CreateRoute(order.PreferredRouteIdentifier, sessionDate, (long)passEntityKey, newRNAOrder);
                                        if (rtEntityKey.HasValue)
                                        {
                                            bool assignedOrderSuccess = AssignOrderToRoute(newRNAOrder, (long)rtEntityKey, 1);
                                            if (assignedOrderSuccess)
                                            {
                                                Logger.InfoFormat("Order {0} assigned to route {1} successfully", newRNAOrder.Identifier, newRNAOrder.PreferredRouteIdentifier);
                                            }
                                            else
                                            {
                                                Logger.ErrorFormat("Error Order {0} assigned to route {1}. See logs and table", newRNAOrder.Identifier, newRNAOrder.PreferredRouteIdentifier);
                                            }
                                        }

                                    }
                                    else
                                    {
                                        Logger.ErrorFormat("Daily Routing Pass for routing session {0} on date {1} not created, cannot create Route", rtSession.Description, rtSession.StartDate);
                                        _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, newRNAOrder.Identifier, "Error assigning Order to Route: No Daily Routing Pass Found for RoutingSession , See Log", "ERROR", out errorMessage, out errorCaught);
                                        if (errorCaught)
                                        {
                                            Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", newRNAOrder.Identifier, errorMessage);
                                        }
                                        else
                                        {
                                            Logger.InfoFormat(" Updating Order Table for order {0} Successful ", newRNAOrder.Identifier, errorMessage);
                                        }
                                    }
                                }
                                else //Error Creating Routing Session
                                {
                                    if (dsSaveResult[0].Error.ValidationFailures.Count() == 0)
                                    {
                                        Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}", order.OrderIdentifier, dsSaveResult[0].Error.Code.ToString());
                                    }
                                    else
                                    {
                                        foreach (ValidationFailure validFailure in dsSaveResult[0].Error.ValidationFailures)
                                        {
                                            Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", order.OrderIdentifier,
                                                dsSaveResult[0].Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                                        }
                                    }

                                }

                            }


                        }
                    }
                    else
                    {
                        Logger.ErrorFormat("Error saving Order {0} to RNA | {1}", order.OrderIdentifier, errorMessage);

                    }
                }
                else //No preferred route Identifier found will add routes to routing session  as unassigned
                {
                    Logger.Error("Order {0} has no preferred route ID, will add to session as unassigned");
                    SaveResult[] dsSaveResult = _ApexConsumer.SaveDailyRoutingSessions(out errorCaught, out errorMessage, new DateTime[] { sessionDate }, new string[] { order.OriginDepotIdentifier }, newRNAOrder);
                    if (dsSaveResult[0].Error == null)
                    {
                        DailyRoutingSession rtSession = (DailyRoutingSession)dsSaveResult[0].Object;
                        Order rnaOrder = newOrdersBag.FirstOrDefault(odr => odr.Identifier.ToUpper() == order.OrderIdentifier.ToUpper());
                        rnaOrder.SessionEntityKey = rtSession.EntityKey;
                        rnaOrder.SessionDescription = rtSession.Description;
                        rnaOrder.SessionDate = rtSession.StartDate;
                        OrderSpec saveOrderSpec = ConvertOrderToOrderSpec(rnaOrder);

                        SaveResult orderSaveResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { saveOrderSpec })[0];
                        if (!errorCaught)
                        {
                            if (orderSaveResult.Error == null)
                            {

                                if (orderSaveResult.Error.ValidationFailures.Count() == 0)
                                {
                                    Logger.ErrorFormat("Error Saving Unassigned Order {0} to RNA | ErrorrCode: {1}", order.OrderIdentifier, dsSaveResult[0].Error.Code.ToString());

                                }
                                else
                                {
                                    foreach (ValidationFailure validFailure in orderSaveResult.Error.ValidationFailures)
                                    {
                                        Logger.ErrorFormat("Error Saving Unassigned Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", order.OrderIdentifier,
                                            dsSaveResult[0].Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                                    }

                                }
                            }
                            else
                            {
                                Logger.InfoFormat("Order {0} saved as Unassigned Order Successfully", rnaOrder.Identifier);
                            }
                        }
                    }
                    else //Error Creating Routing Session
                    {
                        if (dsSaveResult[0].Error.ValidationFailures.Count() == 0)
                        {
                            Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}", order.OrderIdentifier, dsSaveResult[0].Error.Code.ToString());
                            Logger.Error("No Route Created");
                        }
                        else
                        {
                            foreach (ValidationFailure validFailure in dsSaveResult[0].Error.ValidationFailures)
                            {
                                Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", order.OrderIdentifier,
                                    dsSaveResult[0].Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                            }
                            Logger.Error("No Route Created");
                        }

                    }
                }


            }



        }



        private void AddNewDummyOrdersToRNA(List<Order> newOrders, Dictionary<string,long> depotCache)
        {
            List<Order> preppedOrder = new List<Order>();
            bool errorCaught = false;
            string errorMessage = string.Empty;
            System.Collections.Concurrent.ConcurrentBag<Order> newOrdersBag = new System.Collections.Concurrent.ConcurrentBag<Order>(newOrders);
           
            foreach(Order newOrder in newOrders)
            {
                string depotIdentifier = depotCache.FirstOrDefault(x => x.Value == newOrder.RequiredRouteDestinationEntityKey).Key;

                DateTime sessionDate = Convert.ToDateTime(newOrder.BeginDate);
                if (newOrder.PreferredRouteIdentifier != string.Empty || newOrder.PreferredRouteIdentifier != null)
                {

                    Route route = _ApexConsumer.RetrieveRoute(out errorCaught, out errorMessage, newOrder.PreferredRouteIdentifier, sessionDate, depotIdentifier);



                    if (!errorCaught)
                    {
                        if (route != null) // route found, assign order to route
                        {

                            if (newOrdersBag.Any(x => x.Identifier == newOrder.Identifier && x.BeginDate == newOrder.BeginDate))
                            {
                              
                                newOrder.SessionEntityKey = route.RoutingSessionEntityKey;
                                newOrder.SessionDescription = route.RoutingSessionDescription;
                                newOrder.SessionDate = route.RoutingSessionDate;
                            }

                            bool assignedOrderSuccess = AssignDummyOrderToRoute(newOrder, route.EntityKey, route.Version);
                            if (assignedOrderSuccess)
                            {
                                Logger.InfoFormat("Order {0} assigned to route {1} successfully", newOrder.Identifier, newOrder.PreferredRouteIdentifier);
                            }
                            else
                            {
                                Logger.ErrorFormat("Error Order {0} assigned to route {1}. See logs and table", newOrder.Identifier, newOrder.PreferredRouteIdentifier);
                            }

                        }
                        else // Route not found, see if routing session Exist, if not create routing session and route, if it does just create route
                        {
                            DailyRoutingSession dailyRoutingSession = _ApexConsumer.RetrieveDailyRoutingSessionwithOrigin(out errorCaught, out errorMessage, sessionDate, depotIdentifier).DefaultIfEmpty(null).FirstOrDefault();

                            if (dailyRoutingSession != null) // just create route and update order with session information
                            {
                               
                                newOrder.SessionEntityKey = dailyRoutingSession.EntityKey;
                                newOrder.SessionDescription = dailyRoutingSession.Description;
                                newOrder.SessionDate = dailyRoutingSession.StartDate;
                                long? passEntityKey = GetorCreateRoutingSessionPassDummyOrder(dailyRoutingSession, newOrder);
                                if (passEntityKey.HasValue)
                                {
                                    long? rtEntityKey = CreateRouteDummyOrder(newOrder.PreferredRouteIdentifier, sessionDate, (long)passEntityKey, newOrder);

                                    if (rtEntityKey.HasValue)
                                    {
                                        bool assignedOrderSuccess = AssignDummyOrderToRoute(newOrder, (long)rtEntityKey, 1);
                                        if (assignedOrderSuccess)
                                        {
                                            Logger.InfoFormat("Order {0} assigned to route {1} successfully", newOrder.Identifier, newOrder.PreferredRouteIdentifier);
                                        }
                                        else
                                        {
                                            Logger.ErrorFormat("Error Order {0} assigned to route {1}. See logs and table", newOrder.Identifier, newOrder.PreferredRouteIdentifier);
                                        }

                                    }




                                }
                                else
                                {
                                    Logger.ErrorFormat("Daily Routing Pass for routing session {0} on date {1} not created, cannot create Route", dailyRoutingSession.Description, dailyRoutingSession.StartDate);
                                    
                                }



                            }
                            else// session is null create new routing session, create new route, update order with session information;
                            {

                                SaveResult[] dsSaveResult = _ApexConsumer.SaveDailyRoutingSessions(out errorCaught, out errorMessage, new DateTime[] { sessionDate }, new string[] { depotIdentifier }, newOrder);
                                if (dsSaveResult[0].Error == null)
                                {
                                    DailyRoutingSession rtSession = (DailyRoutingSession)dsSaveResult[0].Object;
                                   
                                    newOrder.SessionEntityKey = rtSession.EntityKey;
                                    newOrder.SessionDescription = rtSession.Description;
                                    newOrder.SessionDate = rtSession.StartDate;
                                    long? passEntityKey = GetorCreateRoutingSessionPassDummyOrder(rtSession, newOrder);
                                    if (passEntityKey.HasValue)
                                    {
                                        long? rtEntityKey = CreateRouteDummyOrder(newOrder.PreferredRouteIdentifier, sessionDate, (long)passEntityKey, newOrder);
                                        if (rtEntityKey.HasValue)
                                        {
                                            bool assignedOrderSuccess = AssignDummyOrderToRoute(newOrder, (long)rtEntityKey, 1);
                                            if (assignedOrderSuccess)
                                            {
                                                Logger.InfoFormat("Order {0} assigned to route {1} successfully", newOrder.Identifier, newOrder.PreferredRouteIdentifier);
                                            }
                                            else
                                            {
                                                Logger.ErrorFormat("Error Order {0} assigned to route {1}. See logs and table", newOrder.Identifier, newOrder.PreferredRouteIdentifier);
                                            }
                                        }

                                    }
                                    else
                                    {
                                        Logger.ErrorFormat("Daily Routing Pass for routing session {0} on date {1} not created, cannot create Route", rtSession.Description, rtSession.StartDate);
                                       
                                    }
                                }
                                else //Error Creating Routing Session
                                {
                                    if (dsSaveResult[0].Error.ValidationFailures.Count() == 0)
                                    {
                                        Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}", newOrder.Identifier, dsSaveResult[0].Error.Code.ToString());
                                    }
                                    else
                                    {
                                        foreach (ValidationFailure validFailure in dsSaveResult[0].Error.ValidationFailures)
                                        {
                                            Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", newOrder.Identifier,
                                                dsSaveResult[0].Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                                        }
                                    }

                                }

                            }


                        }
                    }
                    else
                    {
                        Logger.ErrorFormat("Error saving Order {0} to RNA | {1}", newOrder.Identifier, errorMessage);

                    }
                }
                else //No preferred route Identifier found will add routes to routing session  as unassigned
                {
                    Logger.Error("Order {0} has no preferred route ID, will add to session as unassigned");
                    SaveResult[] dsSaveResult = _ApexConsumer.SaveDailyRoutingSessions(out errorCaught, out errorMessage, new DateTime[] { sessionDate }, new string[] { newOrder.Identifier }, newOrder);
                    if (dsSaveResult[0].Error == null)
                    {
                        DailyRoutingSession rtSession = (DailyRoutingSession)dsSaveResult[0].Object;

                        newOrder.SessionEntityKey = rtSession.EntityKey;
                        newOrder.SessionDescription = rtSession.Description;
                        newOrder.SessionDate = rtSession.StartDate;
                        OrderSpec saveOrderSpec = ConvertOrderToOrderSpec(newOrder);

                        SaveResult orderSaveResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { saveOrderSpec })[0];
                        if (!errorCaught)
                        {
                            if (orderSaveResult.Error == null)
                            {

                                if (orderSaveResult.Error.ValidationFailures.Count() == 0)
                                {
                                    Logger.ErrorFormat("Error Saving Unassigned Order {0} to RNA | ErrorrCode: {1}", newOrder.Identifier, dsSaveResult[0].Error.Code.ToString());

                                }
                                else
                                {
                                    foreach (ValidationFailure validFailure in orderSaveResult.Error.ValidationFailures)
                                    {
                                        Logger.ErrorFormat("Error Saving Unassigned Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", newOrder.Identifier,
                                            dsSaveResult[0].Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                                    }

                                }
                            }
                            else
                            {
                                Logger.InfoFormat("Order {0} saved as Unassigned Order Successfully", newOrder.Identifier);
                            }
                        }
                    }
                    else //Error Creating Routing Session
                    {
                        if (dsSaveResult[0].Error.ValidationFailures.Count() == 0)
                        {
                            Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}", newOrder.Identifier, dsSaveResult[0].Error.Code.ToString());
                            Logger.Error("No Route Created");
                        }
                        else
                        {
                            foreach (ValidationFailure validFailure in dsSaveResult[0].Error.ValidationFailures)
                            {
                                Logger.ErrorFormat("Error Creating Routing session for Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", newOrder.Identifier,
                                    dsSaveResult[0].Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                            }
                            Logger.Error("No Route Created");
                        }

                    }
                }


            }



        }



        private TaskServiceWindowOverrideDetail[] ServiceWindowConsolidation(Order RNAOrder, Order dbOrder)
        {


            if ((RNAOrder.Tasks[0].ServiceWindowOverrides.Length == dbOrder.Tasks[0].ServiceWindowOverrides.Length) &&
                (RNAOrder.Tasks[0].ServiceWindowOverrides != null && RNAOrder.Tasks[0].ServiceWindowOverrides.Length != 0))
            {
                dbOrder.Tasks[0].ServiceWindowOverrides.ToList().ForEach(x => x.Action = ActionType.Update);

                for (int i = 0; i < RNAOrder.Tasks[0].ServiceWindowOverrides.Length; i++)
                {
                    dbOrder.Tasks[0].ServiceWindowOverrides[i].EntityKey = RNAOrder.Tasks[0].ServiceWindowOverrides[i].EntityKey;

                }
                RNAOrder.Tasks[0].ServiceWindowOverrides = dbOrder.Tasks[0].ServiceWindowOverrides;
            }
            else if ((dbOrder.Tasks[0].ServiceWindowOverrides == null || dbOrder.Tasks[0].ServiceWindowOverrides.Length == 0) &&
                RNAOrder.Tasks[0].ServiceWindowOverrides.Length > 0)
            {
                RNAOrder.Tasks[0].ServiceWindowOverrides = RNAOrder.Tasks[0].ServiceWindowOverrides.Select(sw => { sw.Action = ActionType.Delete; return sw; }).ToArray();

            }
            else
            {

                if (RNAOrder.Tasks[0].ServiceWindowOverrides.Length == 1 && dbOrder.Tasks[0].ServiceWindowOverrides.Length == 2)
                {


                    dbOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey = RNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;
                    dbOrder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
                    dbOrder.Tasks[0].ServiceWindowOverrides[1].Action = ActionType.Add;
                    RNAOrder.Tasks[0].ServiceWindowOverrides = dbOrder.Tasks[0].ServiceWindowOverrides;

                }
                else if (RNAOrder.Tasks[0].ServiceWindowOverrides.Length == 2 && dbOrder.Tasks[0].ServiceWindowOverrides.Length == 1)
                {

                    dbOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey = RNAOrder.Tasks[0].ServiceWindowOverrides[0].EntityKey;
                    dbOrder.Tasks[0].ServiceWindowOverrides[0].Action = ActionType.Update;
                    dbOrder.Tasks[0].ServiceWindowOverrides[1].Action = ActionType.Delete;
                    dbOrder.Tasks[0].ServiceWindowOverrides[1].EntityKey = RNAOrder.Tasks[0].ServiceWindowOverrides[1].EntityKey;
                    RNAOrder.Tasks[0].ServiceWindowOverrides = dbOrder.Tasks[0].ServiceWindowOverrides;

                }
                else if (RNAOrder.Tasks[0].ServiceWindowOverrides.Length == 0 && dbOrder.Tasks[0].ServiceWindowOverrides.Length > 0)
                {
                    dbOrder.Tasks[0].ServiceWindowOverrides.ToList().ForEach(x => x.Action = ActionType.Add);
                    RNAOrder.Tasks[0].ServiceWindowOverrides = dbOrder.Tasks[0].ServiceWindowOverrides;

                    //for (int i = 0; i < dbOrder.Tasks[0].ServiceWindowOverrides.Length; i++)
                    //{

                    //    RNAOrder.Tasks[0].ServiceWindowOverrides[i] = dbOrder.Tasks[0].ServiceWindowOverrides[i];
                    //    RNAOrder.Tasks[0].ServiceWindowOverrides[i].Action = ActionType.Add;

                    //}



                }



            }


            return RNAOrder.Tasks[0].ServiceWindowOverrides;
        }

        private long? CreateRoute(string createRouteID, DateTime routeStartTime, long dailyPassEntityKey, Order rnaOrder)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            SaveRouteArgs routeArgs = new SaveRouteArgs
            {
                Identifier = createRouteID,
                DispatcherEntityKey = MainService.User.EntityKey,
                OriginDepotEntityKey = (long)rnaOrder.RequiredRouteOriginEntityKey,
                PassEntityKey = dailyPassEntityKey,
                Phase = RoutePhase.Plan,
                Equipment = new RouteEquipmentType[]
                {
                   new RouteEquipmentType
                   {
                       EquipmentTypeEntityKey = (long)_Region.Defaults.EquipmentTypeEntityKey

                   }
               },
                LastStopIsDestination = true,
                RouterEntityKey = MainService.User.EntityKey,
                OriginLoadAction = LoadAction.AsNeeded,
                StartTime = routeStartTime


            };
            SaveRouteResult saveRouteResult = new SaveRouteResult();
            try
            {
                saveRouteResult = _ApexConsumer.CreateRNARoute(out errorCaught, out errorMessage, routeArgs);
                if (!errorCaught)
                {
                    if (saveRouteResult.Error == null)
                    {
                        Logger.InfoFormat("Route {0} Created Successfully", routeArgs.Identifier);
                        return saveRouteResult.EntityKey;
                    }
                    else
                    {
                        string message = string.Empty;
                        if (saveRouteResult.Error.ValidationFailures.Count() == 0)
                        {
                            message = String.Format("Error Creating Routing failed | ErrorCode: {1} ErrorDetail: {1}", saveRouteResult.Error.Code, saveRouteResult.Error.Detail);
                            Logger.ErrorFormat("Error Creating Routing failed | ErrorCode: {1} ErrorDetail: {1}", saveRouteResult.Error.Code, saveRouteResult.Error.Detail);
                            _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order RNA:  " + message + "See Log", "ERROR", out errorMessage, out errorCaught);
                            if (errorCaught)
                            {
                                Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                            }
                            else
                            {
                                Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                            }
                        }

                        else
                        {
                            foreach (ValidationFailure validFailure in saveRouteResult.Error.ValidationFailures)
                            {
                                message = " | " + String.Format("ErrorCode: {0}; ErrorDetailMessage: {1}, ErrorProperty: {2}", "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                                Logger.ErrorFormat("Error Creating Routing failed | ErrorCode: {0}; ErrorDetailMessage: {1}, ErrorProperty: {2}", "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                            }
                            _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order RNA:  " + message + "See Log", "ERROR", out errorMessage, out errorCaught);
                            if (errorCaught)
                            {
                                Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                            }
                            else
                            {
                                Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                            }
                        }
                        return null;
                    }

                }
                else
                {
                    Logger.ErrorFormat("Error Creating Route {0} | {1}", routeArgs.Identifier, errorMessage);
                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Creating Route for Order RNA:  " + errorMessage + "See Log", "ERROR", out errorMessage, out errorCaught);
                    if (errorCaught)
                    {
                        Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                    }
                    else
                    {
                        Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                    }
                    return null;
                }

            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error Creating Route {0} | {1}", routeArgs.Identifier, ex.Message);
                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Creating Route for Order RNA:  " + ex.Message + "See Log", "ERROR", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                }
                else
                {
                    Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                }
                return null;
            }


        }

        private long? CreateRouteDummyOrder(string createRouteID, DateTime routeStartTime, long dailyPassEntityKey, Order rnaOrder)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            SaveRouteArgs routeArgs = new SaveRouteArgs
            {
                Identifier = createRouteID,
                DispatcherEntityKey = MainService.User.EntityKey,
                OriginDepotEntityKey = (long)rnaOrder.RequiredRouteDestinationEntityKey,
                PassEntityKey = dailyPassEntityKey,
                Phase = RoutePhase.Plan,
                Equipment = new RouteEquipmentType[]
                {
                   new RouteEquipmentType
                   {
                       EquipmentTypeEntityKey = (long)_Region.Defaults.EquipmentTypeEntityKey

                   }
               },
                LastStopIsDestination = true,
                RouterEntityKey = MainService.User.EntityKey,
                OriginLoadAction = LoadAction.AsNeeded,
                StartTime = routeStartTime


            };
            SaveRouteResult saveRouteResult = new SaveRouteResult();
            try
            {
                saveRouteResult = _ApexConsumer.CreateRNARoute(out errorCaught, out errorMessage, routeArgs);
                if (!errorCaught)
                {
                    if (saveRouteResult.Error == null)
                    {
                        Logger.InfoFormat("Route {0} Created Successfully", routeArgs.Identifier);
                        return saveRouteResult.EntityKey;
                    }
                    else
                    {
                      
                        if (saveRouteResult.Error.ValidationFailures.Count() == 0)
                        {
                           
                            Logger.ErrorFormat("Error Creating Routing failed | ErrorCode: {1} ErrorDetail: {1}", saveRouteResult.Error.Code, saveRouteResult.Error.Detail);
                            
                        }

                        else
                        {
                            foreach (ValidationFailure validFailure in saveRouteResult.Error.ValidationFailures)
                            {
                               
                                Logger.ErrorFormat("Error Creating Routing failed | ErrorCode: {0}; ErrorDetailMessage: {1}, ErrorProperty: {2}", "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                            }
                          
                            
                        }
                        return null;
                    }

                }
                else
                {
                    Logger.ErrorFormat("Error Creating Route {0} | {1}", routeArgs.Identifier, errorMessage);
                   
                    return null;
                }

            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error Creating Route {0} | {1}", routeArgs.Identifier, ex.Message);
              
                return null;
            }


        }

        private long? GetorCreateRoutingSessionPass(DailyRoutingSession routingSession, Order rnaOrder)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            try
            {

                DailyPass dailyPass = _ApexConsumer.RetrieveRoutingSessionPass(out errorCaught, out errorMessage, routingSession.EntityKey, Config.DefaultRoutingPassIdentifier, _Region.EntityKey);
                if (!errorCaught && dailyPass != null)
                {

                    return dailyPass.EntityKey;
                }
                else if (dailyPass == null && !errorCaught)
                {
                    PassTemplate passTemplate = _ApexConsumer.RetrievePassTemplate(out errorCaught, out errorMessage, Config.DefaultRoutingPassIdentifier, _Region.EntityKey);

                    if (!errorCaught && passTemplate != null)
                    {
                        DailyPass newDailyPass = new DailyPass()
                        {
                            Action = ActionType.Add,
                            CommonAttributes = passTemplate.CommonAttributes,
                            RegionEntityKey = passTemplate.RegionEntityKey,
                            PassTemplateEntityKey = passTemplate.EntityKey,
                            SessionEntityKey = routingSession.EntityKey,
                            Identifier = Config.DefaultRoutingPassIdentifier,
                            CreatedBy = MainService.User.EmailAddress

                        };

                        SaveResult saveResult = _ApexConsumer.CreateDailyPassforSession(out errorCaught, out errorMessage, routingSession,
                            newDailyPass, _Region.EntityKey, (long)_Region.Defaults.EquipmentTypeEntityKey)[0];

                        DailyPass dailypass = (DailyPass)saveResult.Object;
                        if (saveResult.Error == null)
                        {

                            Logger.InfoFormat("Daily Routing Pass {0} Created Succesfully for Routing Session {1} on Date {2}",
                                dailypass.Identifier, routingSession.Description, routingSession.StartDate);
                            return dailypass.EntityKey;
                        }
                        else if (saveResult.Error.ValidationFailures.Count() > 0)
                        {
                            foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                            {
                                Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2}, ErrorProperty: {3} ", dailypass.Identifier,
                                    "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                            }

                            return null;
                        }
                        else
                        {

                            Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2} ", dailypass.Identifier,
                                saveResult.Error.Code, saveResult.Error.Detail);
                            return null;

                        }

                    }
                    else if (!errorCaught && passTemplate == null)
                    {


                        //DailyPass newDailyPass = new DailyPass()
                        //{

                        //    Action = ActionType.Add,
                        //    CommonAttributes = new PassAttributes
                        //    {
                        //        DepotEquipmentTypeQuantities = new DepotEquipmentTypeQuantity[]
                        //        {
                        //                    new DepotEquipmentTypeQuantity
                        //                    {
                        //                        DepotEntityKey = (long)routingSession.DepotEntityKey,
                        //                        EquipmentTypeEntityKey = (long)_Region.Defaults.EquipmentTypeEntityKey,
                        //                        Quantity = 1
                        //                    }
                        //        },
                        //        LoadAction_StartingLoadAction = Enum.GetName(typeof(LoadAction), LoadAction.AsNeeded),
                        //        StartTime = "08:00:00.0000000",
                        //        PreferredRunTime = new TimeSpan(0, 0, 0),
                        //        MaximumRunTime = new TimeSpan(0, 0, 0),
                        //        PreRouteTime = new TimeSpan(0, 0, 0),
                        //        PostRouteTime = new TimeSpan(0, 0, 0),
                        //        ReloadOptions = new ReloadOptions { },
                        //        MultiDayRoutingOptions = new MultiDayRoutingOptions { MultiDayRoutingIsEnabled = false, MultiDayMaxLayovers = 1 }
                                

                        //    },
                        //    RegionEntityKey = _Region.EntityKey,
                        //    SessionEntityKey = routingSession.EntityKey,
                        //    Identifier = Config.DefaultRoutingPassIdentifier,
                        //    CreatedBy = MainService.User.EmailAddress,
                        //    DailyAttributes = new DailyPassAttributes
                        //    {
                        //        Goals = new DailyRoutingGoals
                        //        {
                        //            BasicGoalsMissedTimeWindowFactor = 0,
                        //            BasicGoalsRunTimeBalanceFactor = 0,
                        //            UseAdvancedGoals = false
                        //        }
                        //    }

                        //};

                        //SaveResult saveResult = _ApexConsumer.CreateDailyPassforSession(out errorCaught, out errorMessage, routingSession,
                        //    newDailyPass, _Region.EntityKey, (long)_Region.Defaults.EquipmentTypeEntityKey)[0];
                        //DailyPass dailypass = (DailyPass)saveResult.Object;
                        //if (saveResult.Error == null)
                        //{

                        //    Logger.InfoFormat("Daily Routing Pass {0} Created Succesfully for Routing Session {1} on Date {2}",
                        //        dailypass.Identifier, routingSession.Description, routingSession.StartDate);
                        //    return dailypass.EntityKey;
                        //}
                        //else if (saveResult.Error.ValidationFailures.Count() > 0)
                        //{
                        //    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                        //    {
                        //        Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2}, ErrorProperty: {3} ", dailypass.Identifier,
                        //            "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                        //    }

                        //    return null;
                        //}
                        //else
                        //{

                        //    Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2} ", dailypass.Identifier,
                        //        saveResult.Error.Code, saveResult.Error.Detail);
                        //    return null;
                        //}


                    }
                    //else if (errorCaught) // Create Routing Session Pass
                    //{
                    //    Logger.ErrorFormat("Error Creating Daily Routing Pass | {0}", errorMessage);
                    //    return null;
                    //}

                    Logger.ErrorFormat("Error No Route Pass Template Found {0} ", Config.DefaultRoutingPassIdentifier);
                    return null;

                }
                else if (errorCaught)
                {
                    Logger.ErrorFormat("Error Creating Daily Routing Pass | {0}", errorMessage);
                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Creating Routing Session:  " + errorMessage + "See Log", "ERROR", out errorMessage, out errorCaught);
                    if (errorCaught)
                    {
                        Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                    }
                    else
                    {
                        Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                    }
                    return null;
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error Retrieving or Creating Daily Routing Pass | {0}", ex.Message);
                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Creating Routing Session:  " + ex.Message + "See Log", "ERROR", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                }
                else
                {
                    Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                }
                return null;
            }

            return null;
        }

        private long? GetorCreateRoutingSessionPassDummyOrder(DailyRoutingSession routingSession, Order rnaOrder)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;

            try
            {

                DailyPass dailyPass = _ApexConsumer.RetrieveRoutingSessionPass(out errorCaught, out errorMessage, routingSession.EntityKey, Config.DefaultRoutingPassIdentifier, _Region.EntityKey);
                if (!errorCaught && dailyPass != null)
                {

                    return dailyPass.EntityKey;
                }
                else if (dailyPass == null && !errorCaught)
                {
                    PassTemplate passTemplate = _ApexConsumer.RetrievePassTemplate(out errorCaught, out errorMessage, Config.DefaultRoutingPassIdentifier, _Region.EntityKey);

                    if (!errorCaught && passTemplate != null)
                    {
                        DailyPass newDailyPass = new DailyPass()
                        {
                            Action = ActionType.Add,
                            CommonAttributes = passTemplate.CommonAttributes,
                            RegionEntityKey = passTemplate.RegionEntityKey,
                            PassTemplateEntityKey = passTemplate.EntityKey,
                            SessionEntityKey = routingSession.EntityKey,
                            Identifier = Config.DefaultRoutingPassIdentifier,
                            CreatedBy = MainService.User.EmailAddress

                        };

                        SaveResult saveResult = _ApexConsumer.CreateDailyPassforSession(out errorCaught, out errorMessage, routingSession,
                            newDailyPass, _Region.EntityKey, (long)_Region.Defaults.EquipmentTypeEntityKey)[0];

                        DailyPass dailypass = (DailyPass)saveResult.Object;
                        if (saveResult.Error == null)
                        {

                            Logger.InfoFormat("Daily Routing Pass {0} Created Succesfully for Routing Session {1} on Date {2}",
                                dailypass.Identifier, routingSession.Description, routingSession.StartDate);
                            return dailypass.EntityKey;
                        }
                        else if (saveResult.Error.ValidationFailures.Count() > 0)
                        {
                            foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                            {
                                Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2}, ErrorProperty: {3} ", dailypass.Identifier,
                                    "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                            }

                            return null;
                        }
                        else
                        {

                            Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2} ", dailypass.Identifier,
                                saveResult.Error.Code, saveResult.Error.Detail);
                            return null;

                        }

                    }
                    else if (!errorCaught && passTemplate == null)
                    {


                        //DailyPass newDailyPass = new DailyPass()
                        //{

                        //    Action = ActionType.Add,
                        //    CommonAttributes = new PassAttributes
                        //    {
                        //        DepotEquipmentTypeQuantities = new DepotEquipmentTypeQuantity[]
                        //        {
                        //                    new DepotEquipmentTypeQuantity
                        //                    {
                        //                        DepotEntityKey = (long)routingSession.DepotEntityKey,
                        //                        EquipmentTypeEntityKey = (long)_Region.Defaults.EquipmentTypeEntityKey,
                        //                        Quantity = 1
                        //                    }
                        //        },
                        //        LoadAction_StartingLoadAction = Enum.GetName(typeof(LoadAction), LoadAction.AsNeeded),
                        //        StartTime = "08:00:00.0000000",
                        //        PreferredRunTime = new TimeSpan(0, 0, 0),
                        //        MaximumRunTime = new TimeSpan(0, 0, 0),
                        //        PreRouteTime = new TimeSpan(0, 0, 0),
                        //        PostRouteTime = new TimeSpan(0, 0, 0),
                        //        ReloadOptions = new ReloadOptions { },
                        //        MultiDayRoutingOptions = new MultiDayRoutingOptions { MultiDayRoutingIsEnabled = false, MultiDayMaxLayovers = 1 }


                        //    },
                        //    RegionEntityKey = _Region.EntityKey,
                        //    SessionEntityKey = routingSession.EntityKey,
                        //    Identifier = Config.DefaultRoutingPassIdentifier,
                        //    CreatedBy = MainService.User.EmailAddress,
                        //    DailyAttributes = new DailyPassAttributes
                        //    {
                        //        Goals = new DailyRoutingGoals
                        //        {
                        //            BasicGoalsMissedTimeWindowFactor = 0,
                        //            BasicGoalsRunTimeBalanceFactor = 0,
                        //            UseAdvancedGoals = false
                        //        }
                        //    }

                        //};

                        //SaveResult saveResult = _ApexConsumer.CreateDailyPassforSession(out errorCaught, out errorMessage, routingSession,
                        //    newDailyPass, _Region.EntityKey, (long)_Region.Defaults.EquipmentTypeEntityKey)[0];
                        //DailyPass dailypass = (DailyPass)saveResult.Object;
                        //if (saveResult.Error == null)
                        //{

                        //    Logger.InfoFormat("Daily Routing Pass {0} Created Succesfully for Routing Session {1} on Date {2}",
                        //        dailypass.Identifier, routingSession.Description, routingSession.StartDate);
                        //    return dailypass.EntityKey;
                        //}
                        //else if (saveResult.Error.ValidationFailures.Count() > 0)
                        //{
                        //    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                        //    {
                        //        Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2}, ErrorProperty: {3} ", dailypass.Identifier,
                        //            "ValidationFailure", validFailure.FailureType_Type, validFailure.Property);
                        //    }

                        //    return null;
                        //}
                        //else
                        //{

                        //    Logger.InfoFormat("Error creating daily pass {0} | ErrorCode: {1}, ErrorDetail: {2} ", dailypass.Identifier,
                        //        saveResult.Error.Code, saveResult.Error.Detail);
                        //    return null;
                        //}


                    }
                    //else if (errorCaught) // Create Routing Session Pass
                    //{
                    //    Logger.ErrorFormat("Error Creating Daily Routing Pass | {0}", errorMessage);
                    //    return null;
                    //}

                    Logger.ErrorFormat("Error No Route Pass Template Found ");
                    return null;

                }
                else if (errorCaught)
                {
                    Logger.ErrorFormat("Error Creating Daily Routing Pass | {0}", errorMessage);
                   
                    return null;
                }
            }
            catch (Exception ex)
            {
                Logger.ErrorFormat("Error Retrieving or Creating Daily Routing Pass | {0}", ex.Message);
                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Creating Routing Session:  " + ex.Message + "See Log", "ERROR", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                }
                else
                {
                    Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                }
                return null;
            }

            return null;
        }



        private void DeleteUpdateRNAOrders(string regionID, List<Order> orders)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            List<Order> assignedDeleteOrders = orders.FindAll(order => order.OrderState_State == Enum.GetName(typeof(OrderState), OrderState.Assigned));
            List<Order> unAssignedDeleteOrders = orders.FindAll(order => order.OrderState_State == Enum.GetName(typeof(OrderState), OrderState.Unassigned));
            // unassign delete orders on routes
            try
            {
                foreach (Order order in assignedDeleteOrders)
                {
                    ManipulationResult saveResult = _ApexConsumer.UnassignOrders2(out errorCaught, out errorMessage, new Order[] { order });
                    if (!errorCaught)
                    {
                        if (saveResult.Errors.Count() != 0)
                        {

                            foreach (ManipulationResult.ManipulationError saveResultError in saveResult.Errors)
                            {
                                bool errorUpdatingServiceLocation = false;
                                string errorUpdatingServiceLocationMessage = string.Empty;



                                Logger.Debug("An Error Occured While Unassigning Order. " + order.Identifier + " ErrorCode" + saveResultError.Reason);

                                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, order.Identifier, "Deleting Order failed | Error: " + saveResultError.Reason + " See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                if (errorUpdatingServiceLocation)
                                {
                                    Logger.Debug("Unassigning Order " + order.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                }
                                else
                                {
                                    Logger.Debug("Unassigning Order " + order.Identifier + " error status succesfull");
                                }



                            }
                        }
                        else
                        {
                            Logger.Debug("Unassigning Order : " + order.Identifier + " to RNA Successfull ");
                            unAssignedDeleteOrders.Add(order);

                        }
                    }

                }
            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Unassigning Orders in RNA: " + errorMessage);


            }
            
            try
            {
                foreach (Order dOrder in unAssignedDeleteOrders)
                {
                    dOrder.Action = ActionType.Delete;
                    List<SaveResult> saveOrdersResult = _ApexConsumer.DeleteRNAOrder(out errorCaught, out errorMessage, new Order[] { dOrder }).ToList();
                    if (!errorCaught)
                    {
                        foreach (SaveResult saveResult in saveOrdersResult)
                        {
                            bool errorUpdatingServiceLocation = false;
                            string errorUpdatingServiceLocationMessage = string.Empty;
                            if (saveResult.Error != null)
                            {


                                if (saveResult.Error.Code.ErrorCode_Status == Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {
                                    string message = string.Empty;

                                    foreach (ValidationFailure validFailure in saveResult.Error.ValidationFailures)
                                    {
                                        message = message + " | " + string.Format("ErrorCode: {0} ErrorDetail: {1} ErrorProperty: {2}", "ValidationError", validFailure.FailureType_Type, validFailure.Property);
                                        Logger.Debug("A Validation Error Occured While Deleting Orders. The " + validFailure.Property + " Property for Order " + dOrder.Identifier + " is not Valid");


                                    }
                                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, dOrder.Identifier, "Deleting Order Failed | " + message + "See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                    if (errorUpdatingServiceLocation)
                                    {
                                        Logger.Debug("Updating Order Table | Order " + dOrder.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Order Table | Order  " + dOrder.Identifier + " error status succesfull");
                                    }
                                }
                                else if (saveResult.Error.Code.ErrorCode_Status != Enum.GetName(typeof(ErrorCode), ErrorCode.ValidationFailure))
                                {

                                    Logger.Debug("An Error Occured While Deleting Orders. The " + saveResult.Error.Code.ErrorCode_Status + " Order " + dOrder.Identifier + " is not Valid");

                                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, dOrder.Identifier, "Deleting Order failed | Error: " + saveResult.Error.Code.ErrorCode_Status + " See Log", "ERROR", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                    if (errorUpdatingServiceLocation)
                                    {
                                        Logger.Debug("Updating Order " + dOrder.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                    }
                                    else
                                    {
                                        Logger.Debug("Updating Order " + dOrder.Identifier + " error status succesfull");
                                    }

                                }
                            }
                            else
                            {
                                Logger.Debug("Saving/Updating Order : " + dOrder.Identifier + " to RNA Successfull ");
                                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, dOrder.Identifier, "", "COMPLETE", out errorUpdatingServiceLocationMessage, out errorUpdatingServiceLocation);
                                if (errorUpdatingServiceLocation)
                                {
                                    Logger.Debug("Updating Order " + dOrder.Identifier + " error status in staging table failed | " + errorUpdatingServiceLocationMessage);

                                }
                                else
                                {
                                    Logger.Debug("Updating Order " + dOrder.Identifier + " error status succesfull");
                                }
                            }
                        }

                    }
                    else
                    {
                        Logger.ErrorFormat("Error Caught Saving Orders to RNA : " + errorMessage, dOrder.Identifier);

                    }
                }


            }
            catch (Exception ex)
            {
                errorCaught = true;
                errorMessage = ex.Message;
                Logger.Error("Error Deleting Orders into RNA: " + errorMessage);


            }



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

        private bool SaveUnassignedOrderToRna(Order rnaOrder)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            OrderSpec saveOrderSpec = ConvertOrderToOrderSpec(rnaOrder);

            SaveResult orderSaveResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { saveOrderSpec })[0];
            if (!errorCaught)
            {
                if (orderSaveResult.Error == null)
                {

                    if (orderSaveResult.Error.ValidationFailures.Count() == 0)
                    {
                        Logger.ErrorFormat("Error Saving Unassigned Order {0} to RNA | ErrorrCode: {1}", rnaOrder.Identifier, orderSaveResult.Error.Code.ToString());
                        _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order RNA:  " + orderSaveResult.Error.Code.ToString() + "See Log", "ERROR", out errorMessage, out errorCaught);
                        if (errorCaught)
                        {
                            Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                        }
                        else
                        {
                            Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                        }
                    }
                    else
                    {
                        string message = string.Empty;
                        foreach (ValidationFailure validFailure in orderSaveResult.Error.ValidationFailures)
                        {
                            message = " | " + message + "|" + String.Format("ErrorCode: {0}; ErrorDetailMessage: {1}, ErrorProperty: {2}",
                           orderSaveResult.Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                            Logger.ErrorFormat("Error Saving Unassigned Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", rnaOrder.Identifier,
                               orderSaveResult.Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                        }
                        _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order RNA:  " + message + "See Log", "ERROR", out errorMessage, out errorCaught);
                        if (errorCaught)
                        {
                            Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                        }
                        else
                        {
                            Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                        }
                    }
                    return false;
                }
                else
                {
                    Logger.InfoFormat("Order {0} saved as Unassigned Order Successfully", rnaOrder.Identifier);
                    return true;
                }
            }
            else
            {
                Logger.ErrorFormat("Error Saving Unassigned Order {0} | {1}", rnaOrder.Identifier, errorMessage);
                return false;
            }

        }

        private bool SaveOrderToRna(Order rnaOrder)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            OrderSpec saveOrderSpec = ConvertOrderToOrderSpec(rnaOrder);

            SaveResult orderSaveResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { saveOrderSpec })[0];
            if (!errorCaught)
            {
                if (orderSaveResult.Error == null)
                {

                    if (orderSaveResult.Error.ValidationFailures.Count() == 0)
                    {
                        Logger.ErrorFormat("Error Saving  Order {0} to RNA | ErrorrCode: {1}", rnaOrder.Identifier, orderSaveResult.Error.Code.ToString());

                    }
                    else
                    {
                        string message = string.Empty;
                        foreach (ValidationFailure validFailure in orderSaveResult.Error.ValidationFailures)
                        {
                            message = " | " + message + "|" + String.Format("ErrorCode: {0}; ErrorDetailMessage: {1}, ErrorProperty: {2}",
                               orderSaveResult.Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);

                            Logger.ErrorFormat("Error Saving  Order {0} to RNA | ErrorrCode: {1}; ErrorDetailMessage: {2}, ErrorProperty: {3}", rnaOrder.Identifier,
                               orderSaveResult.Error.Code.ErrorCode_Status, validFailure.FailureType_Type, validFailure.Property);
                        }

                        _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order RNA:  " + message + "See Log", "ERROR", out errorMessage, out errorCaught);
                        if (errorCaught)
                        {
                            Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                        }
                        else
                        {
                            Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                        }

                    }
                    return false;
                }
                else
                {
                    Logger.InfoFormat("Order {0} saved as  Order Successfully", rnaOrder.Identifier);
                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "", "COMPLETE", out errorMessage, out errorCaught);
                    if (errorCaught)
                    {
                        Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                    }
                    else
                    {
                        Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                    }
                    return true;
                }
            }
            else
            {
                Logger.ErrorFormat("Error Saving  Order {0} | {1}", rnaOrder.Identifier, errorMessage);
                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order RNA:  " + errorMessage + "See Log", "ERROR", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                }
                else
                {
                    Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                }
                return false;
            }

        }

        private bool AssignOrderToRoute(Order rnaOrder, long routeEntityKey, long versionNumber)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            OnRouteAutomaticPlacement placement = new OnRouteAutomaticPlacement
            {
                RouteInstance = new DomainInstance
                {
                    EntityKey = routeEntityKey,
                    Version = versionNumber
                },
                AutomaticPlacementGoal_Goal = Enum.GetName(typeof(OnRouteAutomaticPlacement.AutomaticPlacementGoal), OnRouteAutomaticPlacement.AutomaticPlacementGoal.Default),
                IgnoreFlags_RouteExceptionsToIgnore = "MaxTime, Template, RequiredOrigin, LocationEquipmentTypeRestrictions, CellBoundaries",
                ShouldForcePlacement = true,


            };

            RouteRetrievalOptions options = new RouteRetrievalOptions
            {
                EntityKey = routeEntityKey,
                InclusionMode = PropertyInclusionMode.AllWithoutChildren,

            };

            OrderSpec orderSpec = ConvertOrderToOrderSpec(rnaOrder);
            orderSpec.OrderInstance = null;
            SaveResult saveOrderResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { orderSpec }).First();
            if (!errorCaught)
            {
                if (saveOrderResult.Object != null)
                {
                    Order savedRNAOrder = (Order)saveOrderResult.Object;
                    ManipulationResult assignOrderResult = _ApexConsumer.AddOrderToRoute(out errorCaught, out errorMessage, placement, options, savedRNAOrder);
                    if (!errorCaught)
                    {

                        if (assignOrderResult.Errors.Count() > 0)
                        {
                            foreach (ManipulationResult.ManipulationError error in assignOrderResult.Errors)
                            {
                                Logger.ErrorFormat("Assiging Order {0} to Route {1} Failed | {2}", rnaOrder.Identifier, rnaOrder.PreferredRouteIdentifier, error.Reason.ErrorCode_Status);

                                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error assigning Order to Route:  " + error.Reason.ErrorCode_Status + "See Log", "ERROR", out errorMessage, out errorCaught);
                                if (errorCaught)
                                {
                                    Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                                }
                                else
                                {
                                    Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                                }

                            }

                            return false;

                        }
                        else
                        {
                            Logger.InfoFormat("Order {0} assigned to Route {1}", rnaOrder.Identifier, rnaOrder.PreferredRouteIdentifier);
                            _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "", "COMPLETE", out errorMessage, out errorCaught);
                            if (errorCaught)
                            {
                                Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                            }
                            else
                            {
                                Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                            }
                            return true;
                        }


                    }
                    else
                    {

                        Logger.ErrorFormat("Error Saving  Order {0} | {1}", rnaOrder.Identifier, errorMessage);
                        _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Assigning Order to Route " + errorMessage, "Error", out errorMessage, out errorCaught);
                        if (errorCaught)
                        {
                            Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                        }
                        else
                        {
                            Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                        }
                        return false;
                    }
                }
                else
                {
                    _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order to Route " + errorMessage, "Error", out errorMessage, out errorCaught);
                    if (errorCaught)
                    {
                        Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                    }
                    else
                    {
                        Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                    }
                    return false;
                }
            }
            else
            {

                Logger.ErrorFormat("Error Saving  Order {0} | {1}", rnaOrder.Identifier, errorMessage);
                _IntegrationDBAccessor.UpdateOrderStatus(_Region.Identifier, rnaOrder.Identifier, "Error Saving Order to Route " + errorMessage, "Error", out errorMessage, out errorCaught);
                if (errorCaught)
                {
                    Logger.ErrorFormat("Error Updating Order Table for order {0} | Error {1} ", rnaOrder.Identifier, errorMessage);
                }
                else
                {
                    Logger.InfoFormat(" Updating Order Table for order {0} Successful ", rnaOrder.Identifier, errorMessage);
                }
                return false;
            }


        }

        private bool AssignDummyOrderToRoute(Order rnaOrder, long routeEntityKey, long versionNumber)
        {
            bool errorCaught = false;
            string errorMessage = string.Empty;
            OnRouteAutomaticPlacement placement = new OnRouteAutomaticPlacement
            {
                RouteInstance = new DomainInstance
                {
                    EntityKey = routeEntityKey,
                    Version = versionNumber
                },
                AutomaticPlacementGoal_Goal = Enum.GetName(typeof(OnRouteAutomaticPlacement.AutomaticPlacementGoal), OnRouteAutomaticPlacement.AutomaticPlacementGoal.Default),
                IgnoreFlags_RouteExceptionsToIgnore = "MaxTime, Template, RequiredOrigin, LocationEquipmentTypeRestrictions, CellBoundaries",
                ShouldForcePlacement = true,


            };

            RouteRetrievalOptions options = new RouteRetrievalOptions
            {
                EntityKey = routeEntityKey,
                InclusionMode = PropertyInclusionMode.AllWithoutChildren,

            };

            OrderSpec orderSpec = ConvertOrderToOrderSpec(rnaOrder);
            orderSpec.OrderInstance = null;
            SaveResult saveOrderResult = _ApexConsumer.SaveRNAOrders(out errorCaught, out errorMessage, new OrderSpec[] { orderSpec }).First();
            if (!errorCaught)
            {
                if (saveOrderResult.Object != null)
                {
                    Order savedRNAOrder = (Order)saveOrderResult.Object;
                    ManipulationResult assignOrderResult = _ApexConsumer.AddOrderToRoute(out errorCaught, out errorMessage, placement, options, savedRNAOrder);
                    if (!errorCaught)
                    {

                        if (assignOrderResult.Errors.Count() > 0)
                        {
                            foreach (ManipulationResult.ManipulationError error in assignOrderResult.Errors)
                            {
                                Logger.ErrorFormat("Assiging Order {0} to Route {1} Failed | {2}", rnaOrder.Identifier, rnaOrder.PreferredRouteIdentifier, error.Reason.ErrorCode_Status);

                               

                            }

                            return false;

                        }
                        else
                        {
                            Logger.InfoFormat("Order {0} assigned to Route {1}", rnaOrder.Identifier, rnaOrder.PreferredRouteIdentifier);
                            
                            return true;
                        }


                    }
                    else
                    {

                        Logger.ErrorFormat("Error Saving  Order {0} | {1}", rnaOrder.Identifier, errorMessage);
                      
                        return false;
                    }
                }
                else
                {
                  
                    return false;
                }
            }
            else
            {

                Logger.ErrorFormat("Error Saving  Order {0} | {1}", rnaOrder.Identifier, errorMessage);
               
                return false;
            }


        }





    }
}
