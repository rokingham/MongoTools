using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migrate
{
    public class Args
    {
        public static readonly string FULL_COPY        = "full";
        public static readonly string COLLECTIONS_COPY = "collections";
        public static readonly string COLLECTIONS_MASK = "collections-mask";
        public static readonly string COPY_INDEXES     = "copy-indexes";
        public static readonly string DROP_COLLECTIONS = "drop-collections";
    }
}
