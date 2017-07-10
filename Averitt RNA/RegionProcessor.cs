using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Averitt_RNA.Apex;
using Averitt_RNA.DBAccess;
using WindowsServiceUtility;

using System.IO;

namespace Averitt_RNA
{
    class RegionProcessor : Processor
    {

        private Region _Region;
        private ApexConsumer _ApexConsumer;
        private IntegrationDBAccessor _IntegrationDBAccessor;
        static private log4net.ILog _Logger;
        private static DictCache dictCache = new DictCache();
        private static CacheHelper cacheHelper = new CacheHelper();

        public RegionProcessor(Region region) : base(MethodBase.GetCurrentMethod().DeclaringType, region.Identifier)
        {
            _Region = region;
            _ApexConsumer = new ApexConsumer(region, Logger);
            _IntegrationDBAccessor = new IntegrationDBAccessor(Logger);
            
            
        }

       

        public override void Process()
        {
            
            if (!MainService.SessionRequired)
            {
                Logger.Debug("Start Retrieving");

                if ((DateTime.Now.Minute % Config.DictServiceTimeRefresh) == 0)
                {
                    try
                    {
                        Logger.Debug("Writing and Loading Dictionaries");
                        dictCache.resetCache();
                        WriteDictCachedData();
                        LoadDictCachedData();
                        Logger.Debug("Writing and Loading Dictionaries Completed Successfully");
                    }
                    catch (Exception ex)
                    {
                        _Logger.ErrorFormat("Error loading Dict cache file: {0}", ex.Message);
                    }


                }

                //TODO

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

            _Logger.InfoFormat("Writing Dictionary Cache file to {0}", dictCache);
            try
            {
                dictCache.orderClassesDict = _ApexConsumer.RetrieveOrderClassesDict(out errorLevel, out errorMessage);
                dictCache.regionEntityKeyDict = _ApexConsumer.RetrieveRegionEntityKey(out errorLevel, out errorMessage);
                dictCache.serviceTimeEntityKeyDict = _ApexConsumer.RetrieveServiceTimeEntityKey(out errorLevel, out errorMessage);
                dictCache.timeWindowEntityKeyDict = _ApexConsumer.RetrieveTimeWindowEntityKey(out errorLevel, out errorMessage);


                using (StreamWriter writer = new StreamWriter(MainService.dictCacheFile, append: false))
                {
                    Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings
                    {
                        PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.None,
                        ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore
                    };

                    string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(dictCache, Newtonsoft.Json.Formatting.None, settings);
                    
                    writer.Write(jsonData);
                }


            }
            catch (Exception ex)
            {
                _Logger.ErrorFormat("Error writing cache file: {0}", ex.Message);
            }
        }

        public void LoadDictCachedData()
        {
            if (!File.Exists(MainService.dictCacheFile))
            {
                _Logger.Info("No cache file exists");
            }
            else
            {
               _Logger.InfoFormat("Loading cache file from {0}", MainService.dictCacheFile);
                try
                {
                    string jsonData = File.ReadAllText(MainService.dictCacheFile);
                    DictCache temp = Newtonsoft.Json.JsonConvert.DeserializeObject<DictCache>(jsonData);
                    if (temp != null)
                    {
                        dictCache = temp;
                       _Logger.Debug("Dicts loaded from " + MainService.dictCacheFile);
                    }
                }
                catch (Exception ex)
                {
                    _Logger.ErrorFormat("Error opening cache file: {0}", ex.Message);
                }
            }
        }


    }
}
