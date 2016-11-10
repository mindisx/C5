using System;
using SCG = System.Collections.Generic;
using System.Threading;
using System.Runtime.CompilerServices;

namespace C5.concurrent
{
    public class HuntLockDEPQv2<T> : IConcurrentPriorityQueue<T>
    {
        private static object globalLock = new object();
        private class Interval
        {
            internal T element = default(T);
            internal object intervalLock = new object();
            internal int elementTag = -1;
            internal Handle handle = default(Handle);
            public override string ToString()
            {
                return string.Format("[{0} - {1} - min: {2} - max: {3}]", element, elementTag, handle.minindex, handle.maxindex);//, last);
            }
        }

        private class Handle
        {
            internal object minlock = new object();
            internal object maxlock = new object();
            internal int minindex = -1, maxindex = -1;
        }

        private int Available
        {
            get { return -2; }
        }

        private int Empty
        {
            get { return -1; }
        }


        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        Interval[] minheap;
        Interval[] maxheap;
        object[] locks;
        int size;

        public HuntLockDEPQv2() : this(16) { }

        public HuntLockDEPQv2(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            int lenght = 1;
            while (lenght < capacity)
                lenght <<= 1;
            minheap = new Interval[lenght];
            maxheap = new Interval[lenght];
            locks = new object[lenght * 2];
            for (int i = 0; i < lenght; i++)
            {
                minheap[i] = new Interval();
                maxheap[i] = new Interval();

            }

            for (int i = 0; i < lenght * 2; i++)
            {
                if (i < lenght)
                {
                    locks[i] = minheap[i].intervalLock;
                }
                else
                {
                    locks[i] = maxheap[i - lenght].intervalLock;
                }
            }
        }

        /// <summary>
        /// Get the number of elements in the heap
        /// </summary>
        public int Count
        {
            get
            {
                lock (globalLock)
                {
                    return size;
                }
            }
        }

        /// <summary>
        /// Add new item to the heap
        /// </summary>
        /// <param name="item">Item to add to the heap</param>
        /// <returns>boolean</returns>
        public bool Add(T item)
        {
            if (add(item))
                return true;
            return false;
        }

        /// <summary>
        /// Returns a list of all elements not necessary in actual priority
        /// </summary>
        /// <returns>Enumrable object of type T</returns>
        public SCG.IEnumerable<T> All()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Not thread safe. 
        /// Check if the heap fulfills interval heap constraints.
        /// Only blocks new inserts and new deletes. Does not block element reordering.
        /// </summary>
        /// <returns>boolean</returns>
        public bool Check()
        {
            if (size == 0)
                return true;

            if (size == 1)
                return (object)(minheap[0].element) != null && maxheap[0].element != null;

            return checkMin(0) && checkMax(0);
        }

        private bool checkMin(int i)
        {
            bool retval = true;
            Interval min = minheap[i];
            int l = 2 * i + 1;
            int r = l + 1;
            Interval left = null;
            Interval right = null;

            if (l < size && l < minheap.Length)
                left = minheap[l];
            if (r < size && r < minheap.Length)
                right = minheap[r];

            if (left != null)
            {
                if (comparer.Compare(min.element, left.element) > 0)
                    retval = false;
            }
            if (right != null)
            {
                if (comparer.Compare(min.element, right.element) > 0)
                    retval = false;
            }

            if (2 * l < size)
                retval = retval && checkMin(l);

            if (2 * r < size)
                retval = retval && checkMin(r);

            return retval;
        }

        private bool checkMax(int i)
        {
            bool retval = true;
            Interval max = maxheap[i];
            int l = 2 * i + 1;
            int r = l + 1;
            Interval left = null;
            Interval right = null;

            if (l < maxheap.Length)
                left = maxheap[l];
            if (r < maxheap.Length)
                right = maxheap[r];

            if (left != null)
            {
                if (comparer.Compare(max.element, left.element) < 0)
                    retval = false;
            }
            if (right != null)
            {
                if (comparer.Compare(max.element, right.element) < 0)
                    retval = false;
            }

            if (2 * l < size)
                retval = retval && checkMax(l);

            if (2 * r < size)
                retval = retval && checkMax(r);

            return retval;
        }


        private bool deleteMinHeap(Handle handle, int heapsize, ref bool globalLockAcquired)
        {
            bool indexLock = false, iIntervalLockAcquired = false, lastIntervalLockAcquired = false,
                      lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int i = 0;
            Monitor.TryEnter(handle.minlock, 5, ref indexLock);
            if (!indexLock)
                return false;
            try
            {
                if (heapsize == 1)
                {
                    i = handle.minindex;
                    Monitor.TryEnter(minheap[i].intervalLock, 5, ref iIntervalLockAcquired);
                    if (!iIntervalLockAcquired)
                        return false;//retry whole process to prevent dead lock
                    try
                    {
                        minheap[i].element = default(T);
                        minheap[i].handle = default(Handle);
                        minheap[i].elementTag = Empty;
                        return true;
                    }
                    finally
                    {
                        if (iIntervalLockAcquired)
                        {
                            Monitor.Exit(minheap[i].intervalLock);
                            iIntervalLockAcquired = false;
                        }
                    }
                }
                else
                {

                    i = handle.minindex;
                    Monitor.TryEnter(minheap[i].intervalLock, 5, ref iIntervalLockAcquired);
                    if (!iIntervalLockAcquired)
                        return false; //retry whole process to prevent dead lock
                    try
                    {
                        Monitor.TryEnter(maxheap[heapsize - 1].intervalLock, 5, ref lastIntervalLockAcquired);
                        if (!lastIntervalLockAcquired)
                            return false; //retry whole process to prevent dead lock
                        try
                        {
                            minheap[i].element = minheap[heapsize - 1].element;
                            minheap[i].elementTag = Available;
                            minheap[i].handle = minheap[heapsize - 1].handle;
                            minheap[i].handle.minindex = i;
                            minheap[heapsize - 1].element = default(T);
                            minheap[heapsize - 1].handle = default(Handle);
                            minheap[heapsize - 1].elementTag = Empty;

                        }
                        finally
                        {
                            if (lastIntervalLockAcquired)
                            {
                                Monitor.Exit(maxheap[heapsize - 1].intervalLock);
                                lastIntervalLockAcquired = false;
                            }
                        }
                        if (indexLock)
                        {
                            Monitor.Exit(handle.minlock);
                            indexLock = false;
                        }

                        #region  HeapifyMin

                        while (true)
                        {
                            int currentMin = i;
                            int l = 2 * i + 1;
                            int r = l + 1;
                            if (l < minheap.Length)
                                Monitor.Enter(minheap[l].intervalLock, ref lIntervalLockAcquired);

                            if (r < minheap.Length)
                                Monitor.Enter(minheap[r].intervalLock, ref rIntervalLockAcquired);
                            try
                            {
                                if (lIntervalLockAcquired && maxheap[l].elementTag == Empty)
                                {
                                    break;
                                }

                                if (lIntervalLockAcquired && minheap[l].elementTag != Empty)  //if lock was aquired and left child has min element
                                {
                                    if (comparer.Compare(minheap[l].element, minheap[currentMin].element) < 0) //if left child's min node is less
                                        currentMin = l; //left child becomes min node
                                }

                                if (rIntervalLockAcquired && minheap[r].elementTag != Empty) //if lock was aquired and right child has min element
                                {
                                    if (comparer.Compare(minheap[r].element, minheap[currentMin].element) < 0) //if right child's min node is less
                                        currentMin = r;  //right child becomes min node
                                }

                                if (currentMin != i) // if min node is not the parent node...
                                {
                                    swapMinElements(currentMin, i);

                                    if (currentMin == l) //if left child is the node that has min value 
                                    {
                                        if (rIntervalLockAcquired)
                                            Monitor.Exit(minheap[r].intervalLock); //unlock right child
                                    }
                                    else if (currentMin == r) //if right child is the node that has min value 
                                    {
                                        if (lIntervalLockAcquired)
                                            Monitor.Exit(minheap[l].intervalLock); //unlock left child
                                    }

                                    if (iIntervalLockAcquired)
                                    {
                                        Monitor.Exit(minheap[i].intervalLock); //release parent node lock, aka i'th node
                                    }
                                    i = currentMin; //new parent becomes either right or left child.
                                    iIntervalLockAcquired = true; //since one of the child becomes parent, it is still locked
                                    lIntervalLockAcquired = false; //reset lock flag
                                    rIntervalLockAcquired = false; //reset lock flag
                                    continue; //continue with the while loop
                                }
                                break; //exit while loop
                            }
                            finally
                            {
                                if (lIntervalLockAcquired)
                                    Monitor.Exit(minheap[l].intervalLock);
                                if (rIntervalLockAcquired)
                                    Monitor.Exit(minheap[r].intervalLock);
                            }
                        } //end while

                        #endregion

                        if (minheap[i].elementTag == Available)
                            minheap[i].elementTag = Thread.CurrentThread.ManagedThreadId;
                    }
                    finally
                    {
                        if (iIntervalLockAcquired)
                        {
                            Monitor.Exit(minheap[i].intervalLock);
                            iIntervalLockAcquired = false;
                        }
                    }

                    if (minheap[i].elementTag != Empty)
                        bubbleUpMin(i);
                }
            }
            finally
            {
                if (indexLock)
                {
                    Monitor.Exit(handle.minlock);
                    indexLock = false;
                }
            }
            return true;
        }


        private bool deleteMaxHeap(Handle handle, int heapsize, ref bool globalLockAcquired)
        {
            bool indexLock = false, iIntervalLockAcquired = false, lastIntervalLockAcquired = false,
                      lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int i = 0;

            Monitor.TryEnter(handle.maxlock, 5, ref indexLock);
            if (!indexLock)
                return false;
            try
            {
                i = handle.maxindex;
                if (heapsize == 1)
                {
                    Monitor.TryEnter(maxheap[i].intervalLock, 5, ref iIntervalLockAcquired);
                    if (!iIntervalLockAcquired)
                        return false;//retry whole process to prevent dead lock
                    try
                    {
                        maxheap[i].element = default(T);
                        maxheap[i].handle = default(Handle);
                        maxheap[i].elementTag = Empty;
                        return true;
                    }
                    finally
                    {
                        if (iIntervalLockAcquired)
                        {
                            Monitor.Exit(maxheap[i].intervalLock);
                            iIntervalLockAcquired = false;
                        }
                    }
                }
                else
                {
                    Monitor.TryEnter(maxheap[i].intervalLock, 5, ref iIntervalLockAcquired);
                    if (!iIntervalLockAcquired)
                        return false; //retry whole process to prevent dead lock
                    try
                    {
                        Monitor.TryEnter(maxheap[heapsize - 1].intervalLock, 5, ref lastIntervalLockAcquired);
                        if (!lastIntervalLockAcquired)
                            return false; //retry whole process to prevent dead lock
                        try
                        {
                            maxheap[i].element = maxheap[heapsize - 1].element;
                            maxheap[i].elementTag = Available;
                            maxheap[i].handle = maxheap[heapsize - 1].handle;
                            maxheap[i].handle.maxindex = i;
                            maxheap[heapsize - 1].element = default(T);
                            maxheap[heapsize - 1].handle = default(Handle);
                            maxheap[heapsize - 1].elementTag = Empty;
                        }
                        finally
                        {
                            if (lastIntervalLockAcquired)
                            {
                                Monitor.Exit(maxheap[heapsize - 1].intervalLock);
                                lastIntervalLockAcquired = false;
                            }
                        }

                        if (indexLock)
                        {
                            Monitor.Exit(handle.maxlock);
                            indexLock = false;
                        }


                        #region  HeapifyMax
                        while (true)
                        {
                            int currentMax = i;
                            int l = 2 * i + 1;
                            int r = l + 1;
                            if (l < maxheap.Length)
                                Monitor.Enter(maxheap[l].intervalLock, ref lIntervalLockAcquired);

                            if (r < minheap.Length)
                                Monitor.Enter(maxheap[r].intervalLock, ref rIntervalLockAcquired);
                            try
                            {
                                if (lIntervalLockAcquired && maxheap[l].elementTag == Empty)
                                {
                                    break;
                                }
                                if (lIntervalLockAcquired && maxheap[l].elementTag != Empty)  //if lock was aquired and left child has min element
                                {
                                    if (comparer.Compare(maxheap[l].element, maxheap[currentMax].element) > 0) //if left child's min node is less
                                        currentMax = l; //left child becomes min node
                                }

                                if (rIntervalLockAcquired && maxheap[r].elementTag != Empty) //if lock was aquired and right child has min element
                                {
                                    if (comparer.Compare(maxheap[r].element, maxheap[currentMax].element) > 0) //if right child's min node is less
                                        currentMax = r;  //right child becomes min node
                                }

                                if (currentMax != i) // if min node is not the parent node...
                                {
                                    swapMaxElements(currentMax, i);

                                    if (currentMax == l) //if left child is the node that has min value 
                                    {
                                        if (rIntervalLockAcquired)
                                            Monitor.Exit(maxheap[r].intervalLock); //unlock right child
                                    }
                                    else if (currentMax == r) //if right child is the node that has min value 
                                    {
                                        if (lIntervalLockAcquired)
                                            Monitor.Exit(maxheap[l].intervalLock); //unlock left child
                                    }

                                    if (iIntervalLockAcquired)
                                    {
                                        Monitor.Exit(maxheap[i].intervalLock); //release parent node lock, aka i'th node
                                    }
                                    i = currentMax; //new parent becomes either right or left child.
                                    iIntervalLockAcquired = true; //since one of the child becomes parent, it is still locked
                                    lIntervalLockAcquired = false; //reset lock flag
                                    rIntervalLockAcquired = false; //reset lock flag
                                    continue; //continue with the while loop
                                }
                                break; //exit while loop
                            }
                            finally
                            {
                                if (lIntervalLockAcquired)
                                    Monitor.Exit(maxheap[l].intervalLock);
                                if (rIntervalLockAcquired)
                                    Monitor.Exit(maxheap[r].intervalLock);
                            }
                        } //end while

                        #endregion

                        if (maxheap[i].elementTag == Available)
                            maxheap[i].elementTag = Thread.CurrentThread.ManagedThreadId;
                    }
                    finally
                    {
                        if (iIntervalLockAcquired)
                        {
                            Monitor.Exit(maxheap[i].intervalLock);
                            iIntervalLockAcquired = false;
                        }
                    }
                    if (maxheap[i].elementTag != Empty)
                        bubbleUpMax(i);
                }
            }
            finally
            {
                if (indexLock)
                {
                    Monitor.Exit(handle.maxlock);
                    indexLock = false;
                }
            }

            return true;
        }


        public T DeleteMax()
        {
            bool globalLockAcquired = false,
                iIntervalLockAcquired = false,
               lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int i = 0;
            int lastcell = 0;

            T retval;
            while (true)
            {
                i = 0;
                lastcell = 0;
                Monitor.Enter(globalLock, ref globalLockAcquired);
                try
                {
                    if (size == 0)
                    {
                        throw new NoSuchItemException();
                    }

                    if (size == 1) //if there is only one element in the heap, assign it as a return value.
                    {
                        lock (maxheap[0].intervalLock)
                        {
                            retval = maxheap[0].element;
                            if (!deleteMinHeap(maxheap[0].handle, size, ref globalLockAcquired))
                                continue;

                            maxheap[0].element = default(T);
                            maxheap[0].handle = default(Handle);
                            maxheap[0].elementTag = Empty;
                            size--;
                            if (globalLockAcquired)
                            {
                                Monitor.Exit(globalLock);
                                globalLockAcquired = false;
                            }
                            return retval;
                        }
                    }

                    else
                    {
                        Monitor.Enter(maxheap[0].intervalLock, ref iIntervalLockAcquired);
                        try
                        {
                            lastcell = size - 1;
                            lock (maxheap[lastcell].intervalLock)
                            {
                                if (!deleteMinHeap(maxheap[0].handle, size, ref globalLockAcquired))
                                    continue; //retry

                                retval = maxheap[0].element;
                                maxheap[0].element = maxheap[lastcell].element;
                                maxheap[0].elementTag = Available;
                                maxheap[0].handle = maxheap[lastcell].handle;
                                lock (maxheap[0].handle.maxlock)
                                {
                                    maxheap[0].handle.maxindex = 0;
                                }
                                maxheap[lastcell].element = default(T);
                                maxheap[lastcell].handle = default(Handle);
                                maxheap[lastcell].elementTag = Empty;
                                size--;
                                if (globalLockAcquired)
                                {
                                    Monitor.Exit(globalLock);
                                    globalLockAcquired = false;
                                }
                            }

                            #region HeapifyMax

                            i = 0; //node index at which we satrt heapify max
                            while (true)
                            {
                                int currentMax = i;
                                int l = 2 * i + 1;
                                int r = l + 1;


                                if (l < maxheap.Length)
                                    Monitor.Enter(maxheap[l].intervalLock, ref lIntervalLockAcquired);
                                if (r < maxheap.Length)
                                    Monitor.Enter(maxheap[r].intervalLock, ref rIntervalLockAcquired);
                                try
                                {
                                    if (lIntervalLockAcquired && maxheap[l].elementTag == Empty)
                                    {
                                        break;
                                    }
                                    if (lIntervalLockAcquired && maxheap[l].elementTag != Empty)  //if lock was aquired and left child has min element
                                    {
                                        if (comparer.Compare(maxheap[l].element, maxheap[currentMax].element) > 0) //if left child's min node is less
                                            currentMax = l; //left child becomes min node
                                    }

                                    if (rIntervalLockAcquired && maxheap[r].elementTag != Empty) //if lock was aquired and right child has min element
                                    {
                                        if (comparer.Compare(maxheap[r].element, maxheap[currentMax].element) > 0) //if right child's min node is less
                                            currentMax = r;  //right child becomes min node
                                    }

                                    if (currentMax != i) // if min node is not the parent node...
                                    {
                                        swapMaxElements(currentMax, i);

                                        if (currentMax == l) //if left child is the node that has min value 
                                        {
                                            if (rIntervalLockAcquired)
                                                Monitor.Exit(maxheap[r].intervalLock); //unlock right child
                                        }
                                        else if (currentMax == r) //if right child is the node that has min value 
                                        {
                                            if (lIntervalLockAcquired)
                                                Monitor.Exit(maxheap[l].intervalLock); //unlock left child
                                        }

                                        if (iIntervalLockAcquired)
                                        {
                                            Monitor.Exit(maxheap[i].intervalLock); //release parent node lock, aka i'th node
                                        }
                                        i = currentMax; //new parent becomes either right or left child.
                                        iIntervalLockAcquired = true; //since one of the child becomes parent, it is still locked
                                        lIntervalLockAcquired = false; //reset lock flag
                                        rIntervalLockAcquired = false; //reset lock flag
                                        continue; //continue with the while loop
                                    }
                                    break; //exit while loop
                                }
                                finally
                                {
                                    if (lIntervalLockAcquired)
                                        Monitor.Exit(maxheap[l].intervalLock);
                                    if (rIntervalLockAcquired)
                                        Monitor.Exit(maxheap[r].intervalLock);
                                }
                            } //end while

                            #endregion
                        }
                        finally
                        {
                            if (iIntervalLockAcquired)
                            {
                                Monitor.Exit(maxheap[i].intervalLock);
                                iIntervalLockAcquired = false;
                            }
                        }
                    }//else
                }
                finally
                {
                    if (globalLockAcquired)
                    {
                        Monitor.Exit(globalLock);
                        globalLockAcquired = false;
                    }
                }
                break;
            }//while loop
            return retval;
        }


        public T DeleteMin()
        {
            bool globalLockAcquired = false,
                 iIntervalLockAcquired = false,
                lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int i = 0;
            int lastcell = 0;

            T retval;
            while (true)
            {
                i = 0;
                lastcell = 0;
                Monitor.Enter(globalLock, ref globalLockAcquired);
                try
                {
                    if (size == 0)
                        throw new NoSuchItemException();

                    if (size == 1) //if there is only one element in the heap, assign it as a return value.
                    {
                        lock (minheap[0].intervalLock)
                        {
                            if (!deleteMaxHeap(minheap[0].handle, size, ref globalLockAcquired))
                                continue;

                            retval = minheap[0].element;
                            minheap[0].element = default(T);
                            minheap[0].handle = default(Handle);
                            minheap[0].elementTag = Empty;
                            size--;
                            if (globalLockAcquired)
                            {
                                Monitor.Exit(globalLock);
                                globalLockAcquired = false;
                            }
                            return retval;
                        }
                    }

                    else
                    {
                        Monitor.Enter(minheap[0].intervalLock, ref iIntervalLockAcquired);
                        try
                        {
                            lastcell = size - 1;

                            lock (minheap[lastcell].intervalLock)
                            {
                                if (!deleteMaxHeap(minheap[0].handle, size, ref globalLockAcquired))
                                    continue; //retry
                                retval = minheap[0].element;
                                minheap[0].element = minheap[lastcell].element;
                                minheap[0].elementTag = Available;
                                minheap[0].handle = minheap[lastcell].handle;
                                lock (minheap[0].handle.minlock)
                                {
                                    minheap[0].handle.minindex = 0;
                                }
                                minheap[lastcell].element = default(T);
                                minheap[lastcell].handle = default(Handle);
                                minheap[lastcell].elementTag = Empty;
                                size--;
                                if (globalLockAcquired)
                                {
                                    Monitor.Exit(globalLock);
                                    globalLockAcquired = false;
                                }
                            }


                            #region HeapifyMin

                            i = 0; //node index at which we satrt heapify max
                            while (true)
                            {
                                int currentMin = i;
                                int l = 2 * i + 1;
                                int r = l + 1;

                                if (l < minheap.Length)
                                    Monitor.Enter(minheap[l].intervalLock, ref lIntervalLockAcquired);
                                if (r < minheap.Length)
                                    Monitor.Enter(minheap[r].intervalLock, ref rIntervalLockAcquired);
                                try
                                {
                                    if (lIntervalLockAcquired && minheap[l].elementTag == Empty)
                                    {
                                        break;
                                    }
                                    if (lIntervalLockAcquired && minheap[l].elementTag != Empty)  //if lock was aquired and left child has min element
                                    {
                                        if (comparer.Compare(minheap[l].element, minheap[currentMin].element) < 0) //if left child's min node is less
                                            currentMin = l; //left child becomes min node
                                    }

                                    if (rIntervalLockAcquired && minheap[r].elementTag != Empty) //if lock was aquired and right child has min element
                                    {
                                        if (comparer.Compare(minheap[r].element, minheap[currentMin].element) < 0) //if right child's min node is less
                                            currentMin = r;  //right child becomes min node
                                    }

                                    if (currentMin != i) // if min node is not the parent node...
                                    {
                                        swapMinElements(currentMin, i);

                                        if (currentMin == l) //if left child is the node that has min value 
                                        {
                                            if (rIntervalLockAcquired)
                                                Monitor.Exit(minheap[r].intervalLock); //unlock right child
                                        }
                                        else if (currentMin == r) //if right child is the node that has min value 
                                        {
                                            if (lIntervalLockAcquired)
                                                Monitor.Exit(minheap[l].intervalLock); //unlock left child
                                        }

                                        if (iIntervalLockAcquired)
                                        {
                                            Monitor.Exit(minheap[i].intervalLock); //release parent node lock, aka i'th node
                                        }
                                        i = currentMin; //new parent becomes either right or left child.
                                        iIntervalLockAcquired = true; //since one of the child becomes parent, it is still locked
                                        lIntervalLockAcquired = false; //reset lock flag
                                        rIntervalLockAcquired = false; //reset lock flag
                                        continue; //continue with the while loop
                                    }
                                    break; //exit while loop
                                }
                                finally
                                {
                                    if (lIntervalLockAcquired)
                                        Monitor.Exit(minheap[l].intervalLock);
                                    if (rIntervalLockAcquired)
                                        Monitor.Exit(minheap[r].intervalLock);
                                }
                            } //end while

                            #endregion
                        }
                        finally
                        {
                            if (iIntervalLockAcquired)
                            {
                                Monitor.Exit(minheap[i].intervalLock);
                                iIntervalLockAcquired = false;
                            }
                        }
                    }//else
                }
                finally
                {
                    if (globalLockAcquired)
                    {
                        Monitor.Exit(globalLock);
                        globalLockAcquired = false;
                    }
                }
                break;
            }//while loop
            return retval;
        }

        public T FindMax()
        {
            lock (maxheap[0].intervalLock)
                return maxheap[0].element;
        }


        public T FindMin()
        {
            lock (minheap[0].intervalLock)
                return minheap[0].element;
        }

        public bool IsEmpty()
        {
            lock (minheap[0].intervalLock)
            {
                if (minheap[0].elementTag == Empty)
                    return true;
                return false;
            }
        }


        /// <summary>
        /// Increases heap size twice.
        /// </summary>
        /// <returns>an array of interval node locks</returns>
        private object[] resize()
        {
            return (object[])lockAll(() =>
            {
                Interval[] newMinHeap = new Interval[minheap.Length * 2]; //create new heap
                Interval[] newMaxHeap = new Interval[maxheap.Length * 2];
                Array newLocks = Array.CreateInstance(typeof(object), newMinHeap.Length + newMaxHeap.Length); //create new array of locks
                for (int y = 0; y < newMinHeap.Length; y++)
                {
                    if (y < minheap.Length)
                    {
                        newMinHeap[y] = minheap[y]; //add existing intervals to new heap
                        newMaxHeap[y] = maxheap[y];
                    }
                    else
                    {
                        newMinHeap[y] = new Interval(); //fill the rest of new heap with empty intervals
                        newMaxHeap[y] = new Interval(); //fill the rest of new heap with empty intervals
                    }
                }

                for (int y = 0; y < newMinHeap.Length + newMaxHeap.Length; y++)
                {
                    if (y < newMinHeap.Length)
                    {
                        newLocks.SetValue(newMinHeap[y].intervalLock, y); //fill new 
                    }
                    else
                    {
                        newLocks.SetValue(newMaxHeap[y - newMaxHeap.Length].intervalLock, y); //fill new 
                    }
                }
                minheap = newMinHeap;
                maxheap = newMaxHeap;
                return newLocks;
            }); //unlocks all nodes
        }

        /// <summary>
        /// Locks all existing interval nodes 
        /// </summary>
        /// <param name="action">some function that returns Array object</param>
        /// <returns>Array object</returns>
        private Array lockAll(Func<Array> action)
        {
            int s = locks.Length;
            for (int i = 0; i < s; i++)
            {
                Monitor.Enter(locks[i]);
            }
            try
            {
                return action();
            }
            finally
            {
                for (int i = 0; i < s; i++)
                {
                    Monitor.Exit(locks[i]);
                }
            }
        }


        private bool add(T item)
        {
            bool globalLockAcquired = false;
            int s = 0;
            int i = 0;

            lock (globalLock)
            {
                if (size == minheap.Length || size == maxheap.Length)
                {
                    locks = resize(); //resize array and assign new set of locks
                }
                i = s = size;

                Handle handle = new Handle();
                handle.minindex = i;
                handle.maxindex = i;
                lock (minheap[i].intervalLock)
                {
                    lock (maxheap[i].intervalLock)
                    {
                        minheap[i].element = item;
                        minheap[i].elementTag = Thread.CurrentThread.ManagedThreadId; //assign thread id to the element
                        minheap[i].handle = handle;
                        maxheap[i].element = item;
                        maxheap[i].elementTag = Thread.CurrentThread.ManagedThreadId;
                        maxheap[i].handle = handle;
                    }
                }
                size++;
            }

            bubbleUpMin(i);
            bubbleUpMax(i);
            return true;
        }

        private void bubbleUpMin(int i)
        {
            try
            {
                while (i > 0)
                {
                    int p = (i + 1) / 2 - 1;
                    lock (minheap[p].intervalLock)
                    {
                        lock (minheap[i].intervalLock)
                        {
                            if (minheap[p].elementTag == Available && minheap[i].elementTag == Thread.CurrentThread.ManagedThreadId)
                            {
                                if (comparer.Compare(minheap[i].element, minheap[p].element) < 0)
                                {
                                    swapMinElements(i, p);
                                    i = p;
                                }
                                else
                                {
                                    minheap[i].elementTag = Available;  //mark max element available
                                    return; //end bubble up
                                }
                            }
                            else if (minheap[p].elementTag == Empty)
                            {
                                return; // end bubble up
                            }
                            else if (minheap[i].elementTag != Thread.CurrentThread.ManagedThreadId)
                            {
                                i = p;
                            }
                        }//unlock i
                    }//unlock p
                }

                if (i == 0)
                {
                    lock (minheap[i].intervalLock)
                    {
                        if (minheap[i].elementTag == Thread.CurrentThread.ManagedThreadId)
                        {
                            minheap[i].elementTag = Available; //mark max element available
                        }
                    }//unlock i
                }
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        private void bubbleUpMax(int i)
        {
            while (i > 0) //while not root node
            {
                int p = (i + 1) / 2 - 1; //get parent node index
                lock (maxheap[p].intervalLock)
                {
                    lock (maxheap[i].intervalLock)
                    {
                        if (maxheap[p].elementTag == Available && maxheap[i].elementTag == Thread.CurrentThread.ManagedThreadId)
                        {
                            if (comparer.Compare(maxheap[i].element, maxheap[p].element) > 0)
                            {
                                swapMaxElements(i, p);
                                i = p;
                            }
                            else
                            {
                                maxheap[i].elementTag = Available; //mark max element available
                                return; //end bubble up
                            }
                        }
                        else if (maxheap[p].elementTag == Empty)
                        {
                            return; //end bubble up
                        }
                        else if (maxheap[i].elementTag != Thread.CurrentThread.ManagedThreadId)
                        {
                            i = p; //parent node becomes current node
                        }
                    }//unlock i
                }//unlock p
            }
            if (i == 0) //if i is root node
            {
                lock (maxheap[i].intervalLock)
                {
                    if (maxheap[i].elementTag == Thread.CurrentThread.ManagedThreadId)
                    {
                        maxheap[i].elementTag = Available; //mark max element available
                    }
                }//unlock i
            }
        }

        private void swapMaxElements(int cell1, int cell2)
        {
            lock (maxheap[cell2].handle.maxlock)
            {
                lock (maxheap[cell1].handle.maxlock)
                {
                    T element = maxheap[cell2].element;
                    int elementTag = maxheap[cell2].elementTag;
                    Handle handle = maxheap[cell2].handle;
                    maxheap[cell2].element = maxheap[cell1].element;
                    maxheap[cell2].elementTag = maxheap[cell1].elementTag;
                    maxheap[cell2].handle = maxheap[cell1].handle;
                    maxheap[cell2].handle.maxindex = cell2;
                    maxheap[cell1].element = element;
                    maxheap[cell1].elementTag = elementTag;
                    maxheap[cell1].handle = handle;
                    maxheap[cell1].handle.maxindex = cell1;
                }
            }
        }
        private void swapMinElements(int cell1, int cell2)
        {
            lock (minheap[cell2].handle.minlock)
            {
                lock (minheap[cell1].handle.minlock)
                {
                    T element = minheap[cell2].element;
                    int elementTag = minheap[cell2].elementTag;
                    Handle handle = minheap[cell2].handle;
                    minheap[cell2].element = minheap[cell1].element;
                    minheap[cell2].elementTag = minheap[cell1].elementTag;
                    minheap[cell2].handle = minheap[cell1].handle;
                    minheap[cell2].handle.minindex = cell2;
                    minheap[cell1].element = element;
                    minheap[cell1].elementTag = elementTag;
                    minheap[cell1].handle = handle;
                    minheap[cell1].handle.minindex = cell1;
                }
            }
        }
    }
}
