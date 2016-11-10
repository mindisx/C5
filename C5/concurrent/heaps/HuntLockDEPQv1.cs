using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class HuntLockDEPQv1<T> : IConcurrentPriorityQueue<T>
    {
        private readonly object globallock = new object();

        private int Available { get { return -2; } }

        private int Empty { get { return -1; } }

        private class Interval
        {
            internal T element = default(T);
            internal object intervalLock = new object();
            internal int elementTag = -1;
            public override string ToString()
            {
                return string.Format("[{0} - {1}]", element, elementTag);
            }
        }


        private SCG.IComparer<T> comparer;
        private SCG.IEqualityComparer<T> itemEquelityComparer;
        private Interval[] minheap;
        private object[] locks;
        private int size;

        public HuntLockDEPQv1() : this(128) { }

        public HuntLockDEPQv1(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            size = 0;
            int length = 1;
            while (length < capacity)
                length <<= 1;
            minheap = new Interval[length];
            locks = new object[length];
            for (int i = 0; i < length; i++)
            {
                Interval interval = new Interval();
                minheap[i] = interval;
                locks[i] = interval.intervalLock;
            }
        }

        public int Count
        {
            get
            {
                lock (globallock)
                {
                    return size;
                }
            }
        }

        public bool Add(T item)
        {
            if (add(item))
                return true;
            return false;
        }

        public SCG.IEnumerable<T> All()
        {
            throw new NotImplementedException();
        }

        public bool Check()
        {
            throw new NotImplementedException();
        }

        public T DeleteMax()
        {
            T retval;
            int maxIndex = -1;
            bool globalLockAcquired = false;
            Monitor.Enter(globallock, ref globalLockAcquired);
            try
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }
                if (size == 1)
                {
                    lock (minheap[0].intervalLock)
                    {
                        size--;
                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globallock);
                            globalLockAcquired = false;
                        }
                        retval = minheap[0].element;
                        minheap[0].element = default(T);
                        minheap[0].elementTag = Empty;
                        return retval;
                    }
                }

                if (size == 2)
                {
                    lock (minheap[0].intervalLock)
                        lock (minheap[1].intervalLock)
                        {
                            size--;
                            if (globalLockAcquired)
                            {
                                Monitor.Exit(globallock);
                                globalLockAcquired = false;
                            }
                            retval = minheap[1].element;
                            minheap[1].element = default(T);
                            minheap[1].elementTag = Empty;
                            return retval;
                        }
                }

                if (size == 3)
                {
                    lock (minheap[1].intervalLock)
                        lock (minheap[2].intervalLock)
                        {
                            size--;
                            if (globalLockAcquired)
                            {
                                Monitor.Exit(globallock);
                                globalLockAcquired = false;
                            }
                            if (comparer.Compare(minheap[1].element, minheap[2].element) > 0)
                            {
                                retval = minheap[1].element;
                                minheap[1].element = minheap[2].element;
                                minheap[1].elementTag = Available;
                                minheap[2].element = default(T);
                                minheap[2].elementTag = Empty;
                                return retval;
                            }
                            else
                            {
                                retval = minheap[2].element;
                                minheap[2].element = default(T);
                                minheap[2].elementTag = Empty;
                                return retval;
                            }
                        }
                }
                else
                {
                    int s = size;
                    Interval last = new Interval();

                    lock (minheap[s - 1].intervalLock)
                    {
                        last.element = minheap[s - 1].element;
                        minheap[s - 1].element = default(T);
                        minheap[s - 1].elementTag = Empty;
                    }

                    s = s - 1;//minus 1, due to grab above
                    int d = (int)Math.Log(s, 2);
                    int r = s - 1; //point to last element 
                    int l = (r + 1) / 2; //get right element, next to parent
                    bool[] locksAcquired = new bool[(r - l) + 1];
                    for (int i = l; i <= r; i++)
                    {
                        Monitor.Enter(locks[i], ref locksAcquired[i - l]);
                    }
                    try
                    {
                        size--;
                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globallock);
                            globalLockAcquired = false;
                        }
                        maxIndex = l;
                        for (int i = l + 1; i <= r; i++)
                        {
                            if (comparer.Compare(minheap[i].element, minheap[maxIndex].element) > 0)
                            {
                                if (locksAcquired[maxIndex - l])
                                {
                                    Monitor.Exit(locks[maxIndex]);
                                    locksAcquired[maxIndex - l] = false;
                                }
                                maxIndex = i;

                            }
                            else
                            {
                                if (locksAcquired[i - l])
                                {
                                    Monitor.Exit(locks[i]);
                                    locksAcquired[i - l] = false;
                                }
                            }
                        }
                    }
                    finally
                    {
                        for (int i = 0; i < locksAcquired.Length; i++)
                        {
                            if (maxIndex != i + l)
                            {
                                if (locksAcquired[i])
                                {
                                    Monitor.Exit(locks[i + l]);
                                    locksAcquired[i] = true;
                                }
                            }
                        }
                    }
                    if (comparer.Compare(last.element, minheap[maxIndex].element) >= 0)
                    {
                        Monitor.Exit(locks[maxIndex]);
                        retval = last.element;
                        return retval;
                    }
                    else
                    {
                        retval = minheap[maxIndex].element;
                        minheap[maxIndex].element = last.element;
                        minheap[maxIndex].elementTag = Available;
                    }
                }//end if

                int y = maxIndex;

                #region HeapifyMax
                bool yIntervalLockAcquired = true, lIntervalLockAcquired = false,
                    rIntervalLockAcquired = false;
                while (true)
                {
                    try
                    {
                        int currentMax = y;
                        int l = 2 * y + 1;
                        int r = l + 1;

                        if (l < minheap.Length)
                            Monitor.Enter(minheap[l].intervalLock, ref lIntervalLockAcquired);

                        if (r < minheap.Length)
                            Monitor.Enter(minheap[r].intervalLock, ref rIntervalLockAcquired);

                        try
                        {

                            if (lIntervalLockAcquired && minheap[l].elementTag == Empty)
                            {
                                if (lIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[l].intervalLock);
                                    lIntervalLockAcquired = false;
                                }
                                if (rIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[r].intervalLock);
                                    rIntervalLockAcquired = false;
                                }
                                break;
                            }
                            if (lIntervalLockAcquired && minheap[l].elementTag != Empty)  //if lock was aquired and left child has min element
                            {
                                if (comparer.Compare(minheap[l].element, minheap[currentMax].element) > 0) //if left child's min node is less
                                    currentMax = l; //left child becomes min node
                            }

                            if (rIntervalLockAcquired && minheap[r].elementTag != Empty) //if lock was aquired and right child has min element
                            {
                                if (comparer.Compare(minheap[r].element, minheap[currentMax].element) > 0) //if right child's min node is less
                                    currentMax = r;  //right child becomes min node
                            }

                            if (currentMax != y) // if min node is not the parent node...
                            {
                                swapMinElements(currentMax, y);

                                if (currentMax == l) //if left child is the node that has min value 
                                {
                                    if (rIntervalLockAcquired)
                                        Monitor.Exit(minheap[r].intervalLock); //unlock right child
                                }
                                else if (currentMax == r) //if right child is the node that has min value 
                                {
                                    if (lIntervalLockAcquired)
                                        Monitor.Exit(minheap[l].intervalLock); //unlock left child
                                }

                                if (yIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[y].intervalLock); //release parent node lock, aka i'th node
                                }
                                y = currentMax; //new parent becomes either right or left child.
                                yIntervalLockAcquired = true; //since one of the child becomes parent, it is still locked
                                lIntervalLockAcquired = false; //reset lock flag
                                rIntervalLockAcquired = false; //reset lock flag
                                continue; //continue with the while loop
                            }
                            if (y > 0 && minheap[y].elementTag == Available)
                            {
                                minheap[y].elementTag = Thread.CurrentThread.ManagedThreadId;
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
                    }
                    finally
                    {
                        if (yIntervalLockAcquired)
                        {
                            Monitor.Exit(minheap[y].intervalLock); //release parent node lock, aka i'th node
                            yIntervalLockAcquired = false;
                        }
                    }
                } //end while

                if (y > 0)
                {
                    bubbleUpMin(y);
                }
                return retval;
                #endregion
            }
            finally
            {
                if (globalLockAcquired)
                {
                    Monitor.Exit(globallock);
                    globalLockAcquired = false;
                }
            }
        }

        public T DeleteMin()
        {
            bool globalLockAcquired = false, iIntervalLockAcquired = false,
               lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int s = 0, i = 0, lastcell = 0;

            Interval last = new Interval();
            Monitor.Enter(globallock, ref globalLockAcquired);
            try
            {
                T retval;
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }
                s = size;
                lock (minheap[lastcell = s > 0 ? s - 1 : s].intervalLock)
                {
                    size--;
                    if (globalLockAcquired)
                    {
                        Monitor.Exit(globallock); //release lock
                        globalLockAcquired = false; //mark global lock free
                    }

                    last.element = minheap[lastcell].element;
                    minheap[lastcell].element = default(T);
                    minheap[lastcell].elementTag = Empty;
                }

                Monitor.Enter(minheap[i = 0].intervalLock, ref iIntervalLockAcquired); //acquire lock of first interval node and mark it aquired.
                try
                {
                    if (minheap[i].elementTag == Empty)
                        return retval = last.element;

                    retval = minheap[i].element;
                    minheap[i].element = last.element;
                    minheap[i].elementTag = Available;

                    #region Heapify Min

                    int currentMin, l, r;
                    while (true)
                    {
                        currentMin = i;
                        l = 2 * i + 1;
                        r = l + 1;
                        if (l < minheap.Length)
                            Monitor.Enter(minheap[l].intervalLock, ref lIntervalLockAcquired);
                        if (r < minheap.Length)
                            Monitor.Enter(minheap[r].intervalLock, ref rIntervalLockAcquired);
                        try
                        {
                            if (lIntervalLockAcquired && minheap[l].elementTag == Empty)
                            {
                                if (lIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[l].intervalLock);
                                    lIntervalLockAcquired = false;
                                }
                                if (rIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[r].intervalLock);
                                    rIntervalLockAcquired = false;
                                }
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

                            if (currentMin == l) //if left child is the node that has min value 
                            {
                                if (rIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[r].intervalLock); //unlock right child
                                    rIntervalLockAcquired = false;
                                }
                            }
                            else if (currentMin == r) //if right child is the node that has min value 
                            {
                                if (lIntervalLockAcquired)
                                {
                                    Monitor.Exit(minheap[l].intervalLock); //unlock left child
                                    lIntervalLockAcquired = false;
                                }
                            }
                            if (currentMin != i) // if min node is not the parent node...
                            {
                                swapMinElements(currentMin, i);

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
                            {
                                Monitor.Exit(minheap[l].intervalLock);
                                lIntervalLockAcquired = false;
                            }
                            if (rIntervalLockAcquired)
                            {
                                Monitor.Exit(minheap[r].intervalLock);
                                rIntervalLockAcquired = false;
                            }
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
                return retval;
            }
            finally
            {
                if (globalLockAcquired)
                {
                    Monitor.Exit(globallock);
                    globalLockAcquired = false;
                }
            }
        }

        public T FindMax()
        {
            throw new NotImplementedException();
        }

        public T FindMin()
        {
            lock (minheap[0].intervalLock)
            {
                if (minheap[0].elementTag == Empty)
                    throw new NoSuchItemException();
                return minheap[0].element;
            }
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


        private bool add(T item)
        {
            int s = 0, i = 0;
            bool globalLockAcquired = false;
            Monitor.Enter(globallock, ref globalLockAcquired);
            try
            {
                if (size == minheap.Length)
                {
                    locks = resize(); //resize array and assign new set of locks
                }

                i = s = size;
                size++;
                lock (minheap[i].intervalLock)
                {
                    if (globalLockAcquired)
                    {
                        Monitor.Exit(globallock);
                        globalLockAcquired = false;
                    }

                    minheap[i].element = item;
                    minheap[i].elementTag = Thread.CurrentThread.ManagedThreadId; //assign thread id to the element
                }
                bubbleUpMin(i);
            }
            finally
            {
                if (globalLockAcquired)
                {
                    Monitor.Exit(globallock);
                    globalLockAcquired = false;
                }
            }
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

        private void swapMinElements(int cell1, int cell2)
        {

            T element = minheap[cell2].element;
            int elementTag = minheap[cell2].elementTag;

            minheap[cell2].element = minheap[cell1].element;
            minheap[cell2].elementTag = minheap[cell1].elementTag;

            minheap[cell1].element = element;
            minheap[cell1].elementTag = elementTag;

        }

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

        /// <summary>
        /// Increases heap size twice.
        /// </summary>
        /// <returns>an array of interval node locks</returns>
        private object[] resize()
        {
            return (object[])lockAll(() =>
            {
                Interval[] newMinHeap = new Interval[minheap.Length * 2]; //create new heap
                Array newLocks = Array.CreateInstance(typeof(object), newMinHeap.Length); //create new array of locks
                for (int y = 0; y < newMinHeap.Length; y++)
                {
                    if (y < minheap.Length)
                    {
                        newMinHeap[y] = minheap[y]; //add existing intervals to new heap
                        newLocks.SetValue(newMinHeap[y].intervalLock, y);
                    }
                    else
                    {
                        newMinHeap[y] = new Interval(); //fill the rest of new heap with empty intervals
                        newLocks.SetValue(newMinHeap[y].intervalLock, y);
                    }

                }
                minheap = newMinHeap;
                return newLocks;
            }); //unlocks all nodes
        }
    }
}
