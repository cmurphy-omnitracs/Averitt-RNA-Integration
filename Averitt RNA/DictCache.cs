using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Averitt_RNA.Apex;
using System.IO;
using System.Runtime.Caching;
using System.ServiceModel;

namespace Averitt_RNA
{
    public class DictCache
    {
        #region Private Members

        private long _BusinessUnitEntityKey;
        private Region _Region;
        private RegionContext _RegionContext;
        private log4net.ILog _Logger;
        private QueryServiceClient _QueryServiceClient;
        private MappingServiceClient _MappingServiceClient;
        private RoutingServiceClient _RoutingServiceClient;
        private CacheHelper dictCache = new CacheHelper();

        #endregion

        #region Public Members

        public Dictionary<string, long> regionEntityKeyDict { get; set; }
        public Dictionary<string, long> serviceTimeEntityKeyDict { get; set; }
        public Dictionary<string, long> timeWindowEntityKeyDict { get; set; }
        public Dictionary<string, long> depotsForRegionDict { get; set; }
        public Dictionary<string, long> orderClassesDict { get; set; }


        #endregion

        #region Public Methods

        public void resetCache()
        {

            regionEntityKeyDict = new Dictionary<string, long>();
            serviceTimeEntityKeyDict = new Dictionary<string, long>();
            timeWindowEntityKeyDict= new Dictionary<string, long>();
            depotsForRegionDict = new Dictionary<string, long>();
            orderClassesDict = new Dictionary<string, long>();

        }
        
        #endregion
    }



}
