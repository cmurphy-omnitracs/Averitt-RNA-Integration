using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Averitt_RNA.Apex;
using System.IO;
using System.Runtime.Caching;
using System.ServiceModel;
using System.Threading;

namespace Averitt_RNA
{
    public class DictCache
    {
        #region Private Members

        private long _BusinessUnitEntityKey;
        private Region _Region;
        private RegionContext _RegionContext;
        private log4net.ILog _Logger;
        private CacheHelper dictCache = new CacheHelper();
        private ApexConsumer _ApexConsumer;
        private string dicFilePath;
        #endregion

        #region Public Members

        public Dictionary<string, long> regionEntityKeyDict { get; set; }
        public Dictionary<string, long> serviceTimeEntityKeyDict { get; set; }
        public Dictionary<string, long> timeWindowEntityKeyDict { get; set; }
        public Dictionary<string, long> depotsForRegionDict { get; set; }
        public Dictionary<string, long> orderClassesDict { get; set; }
        public Dictionary<string, ServiceLocation> serviceLocationDict { get; set; }


        #endregion

        #region Public Methods
        public DictCache(string filePath, log4net.ILog logger)
        {
            dicFilePath = filePath;
            _Logger = logger;

        }
        public void resetCache()
        {

            regionEntityKeyDict.Clear();
            serviceTimeEntityKeyDict.Clear();
            timeWindowEntityKeyDict.Clear();
            depotsForRegionDict.Clear();
            orderClassesDict.Clear();
            serviceLocationDict.Clear();

        }

        public void WriteDictCachedData(long regionEntityKey)
        {



            ApexConsumer.ErrorLevel errorLevel = ApexConsumer.ErrorLevel.None;
            string errorMessage = string.Empty;


            _Logger.InfoFormat("Writing Dictionary Cache file to {0}", dicFilePath);


            try
            {
                depotsForRegionDict = _ApexConsumer.RetrieveDepotsForRegion(out errorLevel, out errorMessage, regionEntityKey);
                orderClassesDict = _ApexConsumer.RetrieveOrderClassesDict(out errorLevel, out errorMessage);
                regionEntityKeyDict = _ApexConsumer.RetrieveRegionEntityKey(out errorLevel, out errorMessage);
                serviceTimeEntityKeyDict = _ApexConsumer.RetrieveServiceTimeEntityKey(out errorLevel, out errorMessage);
                timeWindowEntityKeyDict = _ApexConsumer.RetrieveTimeWindowEntityKey(out errorLevel, out errorMessage);
                
                Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings
                {
                    PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects,
                    ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore

                };



                string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(dictCache, Newtonsoft.Json.Formatting.None, settings);


                // StreamWriter writer = new StreamWriter(dictCacheFile, append: false);
                System.IO.File.WriteAllText(dicFilePath, jsonData);



                // writer.Write(jsonData);

            }


            catch (Exception ex)
            {
                _Logger.ErrorFormat("Error writing cache file: {0}", ex.Message);
            }


        }

        public void LoadDictCachedData()
        {
            if (!File.Exists(dicFilePath))
            {
                _Logger.Info("No cache file exists");
            }
            else
            {
                _Logger.InfoFormat("Loading cache file from {0}", dicFilePath);
                try
                {
                    string jsonData = File.ReadAllText(dicFilePath);
                    DictCache temp = Newtonsoft.Json.JsonConvert.DeserializeObject<DictCache>(jsonData);
                    if (temp != null)
                    {
                        this.depotsForRegionDict = temp.depotsForRegionDict;
                        this.orderClassesDict = temp.orderClassesDict;
                        this.regionEntityKeyDict = temp.regionEntityKeyDict;
                        this.serviceLocationDict = temp.serviceLocationDict;
                        this.serviceTimeEntityKeyDict = temp.serviceTimeEntityKeyDict;
                        this.timeWindowEntityKeyDict = temp.timeWindowEntityKeyDict;
                        
                        _Logger.Debug("Dicts loaded from " + dicFilePath);
                    }
                }
                catch (Exception ex)
                {
                    _Logger.ErrorFormat("Error opening cache file: {0}", ex.Message);
                }
            }
        }


        #endregion
    }



}
