using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoDB
{
    /// <summary>
    /// Enum of possible Copy Modes (used to avoid messing with "Strings" or "Ints"
    /// </summary>
    public enum CopyMode
    {
        FullDatabaseCopy, CollectionsCopy, CollectionsMaskCopy
    }

    /// <summary>
    /// Enum of Possible Duplicate Modes (used to avoid messing with "Strings" or "Ints"
    /// </summary>
    public enum DuplicateMode
    {
        DuplicateCollections, DuplicateCollectionsWithMask
    }
}
