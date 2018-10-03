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
        public Dictionary<string, Depot> depotsForRegionDict { get; set; }
        public Dictionary<string, long> orderClassesDict { get; set; }
        public Dictionary<string, ServiceLocation> serviceLocationDict { get; set; }


        #endregion

        #region Public Methods
        public DictCache(string filePath, log4net.ILog logger, Region region, ApexConsumer apexConsumer)
        {
            dicFilePath = filePath;
            _Logger = logger;
            regionEntityKeyDict = new Dictionary<string, long>();
            serviceLocationDict = new Dictionary<string, ServiceLocation>();
            timeWindowEntityKeyDict = new Dictionary<string, long>();
            serviceTimeEntityKeyDict = new Dictionary<string, long>();
            depotsForRegionDict = new Dictionary<string, Depot>();
            orderClassesDict = new Dictionary<string, long>();
            _ApexConsumer = apexConsumer;
            _Region = region;
        }
        public void resetCache()
        {

            this.regionEntityKeyDict.Clear();
            this.serviceTimeEntityKeyDict.Clear();
            this.timeWindowEntityKeyDict.Clear();
            this.depotsForRegionDict.Clear();
            this.orderClassesDict.Clear();
            this.serviceLocationDict.Clear();

        }

        public void WriteDictCachedData(long regionEntityKey)
        {



            ApexConsumer.ErrorLevel errorLevel = ApexConsumer.ErrorLevel.None;
            string errorMessage = string.Empty;


            _Logger.InfoFormat("Writing Dictionary Cache file to {0}", dicFilePath);


            try
            {
                long[] regEntityKey = new long[]{ _Region.EntityKey};

               this.depotsForRegionDict = _ApexConsumer.RetrieveDepotsForRegion(out errorLevel, out errorMessage, regionEntityKey);
               this.orderClassesDict = _ApexConsumer.RetrieveOrderClassesDict(out errorLevel, out errorMessage);
               this.regionEntityKeyDict = _ApexConsumer.RetrieveRegionEntityKey(out errorLevel, out errorMessage);
               this.serviceTimeEntityKeyDict = _ApexConsumer.RetrieveServiceTimeEntityKey(out errorLevel, out errorMessage);
               this.timeWindowEntityKeyDict = _ApexConsumer.RetrieveTimeWindowEntityKey(out errorLevel, out errorMessage);
                List<ServiceLocation> locations = _ApexConsumer.RetrieveServiceLocationsByRegion(out errorLevel, out errorMessage, regEntityKey).ToList();

                this.serviceLocationDict = locations.ToDictionary(x => x.Identifier, y => y);
                Newtonsoft.Json.JsonSerializerSettings settings = new Newtonsoft.Json.JsonSerializerSettings
                {
                    PreserveReferencesHandling = Newtonsoft.Json.PreserveReferencesHandling.Objects,
                    ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore

                };



                string jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(this, Newtonsoft.Json.Formatting.None, settings);


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


        public void refreshServiceLocation()
        {



            ApexConsumer.ErrorLevel errorLevel = ApexConsumer.ErrorLevel.None;
            string errorMessage = string.Empty;


            _Logger.InfoFormat("Refresh Service Location Cache file to {0}", dicFilePath);


            try
            {
                long[] regEntityKey = new long[] { _Region.EntityKey };

                List<ServiceLocation> locations = _ApexConsumer.RetrieveServiceLocationsByRegion(out errorLevel, out errorMessage, regEntityKey).ToList();

                this.serviceLocationDict = locations.ToDictionary(x => x.Identifier, y => y);
               

            }


            catch (Exception ex)
            {
                _Logger.ErrorFormat("Error refreshing service location cache file: {0}", ex.Message);
            }


        }


        #endregion
    }



}
