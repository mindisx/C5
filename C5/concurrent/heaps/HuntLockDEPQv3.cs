using System;
using SCG = System.Collections.Generic;
using System.Threading;
using System.Runtime.CompilerServices;

namespace C5.concurrent
{
    public class HuntLockDEPQv3<T> : IConcurrentPriorityQueue<T>
    {
        private class Interval
        {
            internal T first = default(T), last = default(T);
            internal object intervalLock = new object();
            internal int firstTag = -1, lastTag = -1;

            public override string ToString()
            {
                return string.Format("[{0} - {1}; {2} - {3}]", first, firstTag, last, lastTag);
            }
        }

        private int Available
        {
            get { return -2; }
        }


        private int Empty
        {
            get { return -1; }
        }


        private static object globalLock = new object();

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        Interval[] heap;
        object[] locks;
        int size;

        public HuntLockDEPQv3() : this(16) { }

        public HuntLockDEPQv3(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            int lenght = 1;
            while (lenght < capacity) lenght <<= 1;
            heap = new Interval[lenght];
            locks = new object[lenght];
            for (int i = 0; i < lenght; i++)
            {
                heap[i] = new Interval();
                locks[i] = heap[i].intervalLock;
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
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();

                return (T[])lockAll(() =>
                {
                    T[] elements = new T[size];
                    int counter = 0;
                    for (int i = 0; i < size; i++)
                    {
                        if (i % 2 == 0)
                        {
                            elements[counter] = heap[i / 2].last;
                            counter++;
                        }
                        else
                        {
                            elements[counter] = heap[i / 2].first;
                            counter++;
                        }
                    }
                    return elements;
                }); //unlock all inteval nodes and return an array of type T
            } // unlock heap
        }

        /// <summary>
        /// Not thread safe. 
        /// Check if the heap fulfills interval heap constraints.
        /// Only blocks new inserts and new deletes. Does not block element reordering.
        /// </summary>
        /// <returns>boolean</returns>
        public bool Check()
        {
            lock (globalLock)
            {
                if (size == 0)
                    return true;

                if (size == 1)
                    return (object)(heap[0].first) != null;

                return check(0, heap[0].first, heap[0].last);
            } //unlock heap
        }

        public T DeleteMax()
        {
            bool globalLockAcquired = false, iIntervalLockAcquired = false,
                lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int i = 0;

            Monitor.Enter(globalLock, ref globalLockAcquired); //acquire global lock and mark it aquired.
            try
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }

                T retval;
                if (size == 1) //if there is only one element in the heap, assign it as a return value.
                {
                    lock (heap[0].intervalLock) //lock 
                    {
                        size = 0;
                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globalLock); //exit global lock
                            globalLockAcquired = false;
                        }
                        retval = heap[0].first;
                        heap[0].first = default(T);
                        heap[0].firstTag = Empty;
                    }
                }
                else
                {
                    Interval last = new Interval();
                    int s = size;
                    int lastcell = (size - 1) / 2;
                    lock (heap[lastcell].intervalLock)
                    {
                        size--;
                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globalLock); //exit global lock
                            globalLockAcquired = false;
                        }
                        if (s % 2 == 0)
                        {
                            last.last = heap[lastcell].last;
                            heap[lastcell].last = default(T);
                            heap[lastcell].lastTag = Empty;
                        }
                        else
                        {
                            last.last = heap[lastcell].first;
                            heap[lastcell].first = default(T);
                            heap[lastcell].firstTag = Empty;
                        }
                    }

                    Monitor.Enter(heap[0].intervalLock, ref iIntervalLockAcquired); //acquire lock of first interval node and mark it aquired.
                    try
                    {
                        if (heap[0].firstTag == Empty)
                        {
                            retval = last.last;
                            return retval;
                        }
                        if (heap[0].lastTag == Empty)
                        {
                            if (comparer.Compare(last.last, heap[0].first) >= 0)
                            {
                                retval = last.last;
                                return retval;
                            }
                            else
                            {
                                retval = heap[0].first;
                                heap[0].first = last.last;
                                heap[0].firstTag = Available;
                                return retval;
                            }
                        }

                        if (comparer.Compare(last.last, heap[0].last) >= 0)
                        {
                            retval = last.last;
                            return retval;
                        }
                        else
                        {
                            retval = heap[0].last;
                            heap[0].last = last.last;
                            heap[0].lastTag = Available;
                        }

                        #region Heapify max

                        i = 0; //node index at which we satrt heapify max
                        while (true)
                        {
                            if (heap[i].lastTag != Empty && comparer.Compare(heap[i].last, heap[i].first) < 0)
                            {
                                T first = heap[i].first;
                                heap[i].first = heap[i].last;
                                heap[i].last = first;

                            }
                            int currentMax = i;
                            int l = 2 * i + 1;
                            int r = l + 1;
                            bool firstMax = false;

                            if (l < heap.Length)
                                Monitor.Enter(heap[l].intervalLock, ref lIntervalLockAcquired);

                            if (r < heap.Length) //try to obtain a lock on righ child if exist
                                Monitor.Enter(heap[r].intervalLock, ref rIntervalLockAcquired);
                            try
                            {
                                if (lIntervalLockAcquired && heap[l].firstTag == Empty)
                                    break;

                                if (lIntervalLockAcquired && heap[l].lastTag != Empty)  //if lock was aquired and left child has min and max element
                                {
                                    if (comparer.Compare(heap[l].last, heap[currentMax].last) > 0) //if left child's max node is greather
                                        currentMax = l; //left child becomes max node
                                }
                                else if (lIntervalLockAcquired && heap[l].lastTag == Empty) //if lock was aquired and left child only has min element
                                {
                                    if (comparer.Compare(heap[l].first, heap[currentMax].last) > 0) // if left child's min node is greater
                                    {
                                        currentMax = l; //left child becomes max node
                                        firstMax = true; //indicate that node's min element is max.
                                    }
                                }

                                if (rIntervalLockAcquired && heap[r].lastTag != Empty) //if lock was aquired and lright child has min and max element
                                {
                                    if (comparer.Compare(heap[r].last, heap[currentMax].last) > 0) //if right child's max node is greather
                                        currentMax = r;  //right child becomes max node
                                }
                                else if (rIntervalLockAcquired && heap[r].lastTag == Empty) //if lock was aquired and right child only has min element
                                {
                                    if (comparer.Compare(heap[r].first, heap[currentMax].last) > 0)
                                    {
                                        currentMax = r; //right child becomes max node
                                        firstMax = true; //indicate that node's min element is max.
                                    }
                                }

                                if (currentMax != i) // if max node is not the parent node...
                                {
                                    if (firstMax)
                                    {
                                        T first = heap[currentMax].first;
                                        heap[currentMax].last = heap[i].last;
                                        heap[i].last = first;
                                        //swapFirstWithLast(currentMax, i);
                                    }
                                    else
                                        swapLastWithLast(currentMax, i);

                                    if (currentMax == l) //if left child is the node that has max value 
                                    {
                                        if (rIntervalLockAcquired)
                                            Monitor.Exit(heap[r].intervalLock); //unlock right child
                                    }
                                    else if (currentMax == r) //if right child is the node that has max value 
                                    {
                                        if (lIntervalLockAcquired)
                                            Monitor.Exit(heap[l].intervalLock); //unlock left child
                                    }

                                    if (iIntervalLockAcquired)
                                    {
                                        Monitor.Exit(heap[i].intervalLock); //release parent node lock, aka i'th node
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
                                    Monitor.Exit(heap[l].intervalLock);
                                if (rIntervalLockAcquired)
                                    Monitor.Exit(heap[r].intervalLock);
                            }
                        } //end while

                        #endregion
                    }
                    finally
                    {
                        if (iIntervalLockAcquired)
                            Monitor.Exit(heap[i].intervalLock);
                    }
                } //end else
                return retval;
            }
            finally
            {
                if (globalLockAcquired)
                {
                    Monitor.Exit(globalLock); //exit global lock
                    globalLockAcquired = false;
                }
            }
        }

        public T DeleteMin()
        {
            bool globalLockAcquired = false, iIntervalLockAcquired = false,
              lIntervalLockAcquired = false, rIntervalLockAcquired = false;
            int i = 0;

            Monitor.Enter(globalLock, ref globalLockAcquired);
            try
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }

                T retval;
                if (size == 1) //if there is only one element in the heap, assign it as a return value.
                {
                    lock (heap[0].intervalLock) //lock 
                    {
                        size = 0;
                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globalLock); //exit global lock
                            globalLockAcquired = false;
                        }
                        retval = heap[0].first;
                        heap[0].first = default(T);
                        heap[0].firstTag = Empty;
                        return retval;
                    }
                }
                else
                {
                    Interval last = new Interval();
                    int s = size;
                    int lastcell = (size - 1) / 2;
                    lock (heap[lastcell].intervalLock)
                    {
                        size--;
                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globalLock); //exit global lock
                            globalLockAcquired = false;
                        }
                        if (s % 2 == 0)
                        {
                            last.first = heap[lastcell].last;
                            heap[lastcell].last = default(T);
                            heap[lastcell].lastTag = Empty;
                        }
                        else
                        {
                            last.first = heap[lastcell].first;
                            heap[lastcell].first = default(T);
                            heap[lastcell].firstTag = Empty;
                        }
                    }


                    Monitor.Enter(heap[0].intervalLock, ref iIntervalLockAcquired); //acquire lock of first interval node and mark it aquired.
                    try
                    {
                        if (heap[0].firstTag == Empty)
                        {
                            retval = last.first;
                            return retval;
                        }

                        if (comparer.Compare(last.first, heap[0].first) <= 0)
                        {
                            retval = last.first;
                            return retval;
                        }
                        else
                        {
                            retval = heap[0].first;
                            heap[0].first = last.first;
                            heap[0].firstTag = Available;
                        }

                        #region Heapify Min

                        i = 0; //node index at which we satrt heapify max
                        while (true)
                        {
                            if (heap[i].lastTag != Empty && comparer.Compare(heap[i].first, heap[i].last) > 0)
                            {
                                T first = heap[i].first;
                                heap[i].first = heap[i].last;
                                heap[i].last = first;
                            }

                            int currentMin = i;
                            int l = 2 * i + 1;
                            int r = l + 1;

                            if (l < heap.Length)//try to obtain a lock on left child if exist
                                Monitor.Enter(heap[l].intervalLock, ref lIntervalLockAcquired);

                            if (r < heap.Length)
                                Monitor.Enter(heap[r].intervalLock, ref rIntervalLockAcquired);

                            try
                            {
                                if (lIntervalLockAcquired && heap[l].firstTag == Empty)
                                {
                                    break;
                                }
                                if (lIntervalLockAcquired && heap[l].firstTag != Empty)  //if lock was aquired and left child has min element
                                {
                                    if (comparer.Compare(heap[l].first, heap[currentMin].first) < 0) //if left child's min node is less
                                        currentMin = l; //left child becomes min node
                                }

                                if (rIntervalLockAcquired && heap[r].firstTag != Empty) //if lock was aquired and right child has min element
                                {
                                    if (comparer.Compare(heap[r].first, heap[currentMin].first) < 0) //if right child's min node is less
                                        currentMin = r;  //right child becomes min node
                                }

                                if (currentMin != i) // if min node is not the parent node...
                                {
                                    swapFirstWithFirst(currentMin, i);

                                    if (currentMin == l) //if left child is the node that has min value 
                                    {
                                        if (rIntervalLockAcquired)
                                            Monitor.Exit(heap[r].intervalLock); //unlock right child
                                    }
                                    else if (currentMin == r) //if right child is the node that has min value 
                                    {
                                        if (lIntervalLockAcquired)
                                            Monitor.Exit(heap[l].intervalLock); //unlock left child
                                    }

                                    if (iIntervalLockAcquired)
                                    {
                                        Monitor.Exit(heap[i].intervalLock); //release parent node lock, aka i'th node
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
                                    Monitor.Exit(heap[l].intervalLock);
                                    lIntervalLockAcquired = false;
                                }

                                if (rIntervalLockAcquired)
                                {
                                    Monitor.Exit(heap[r].intervalLock);
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
                            Monitor.Exit(heap[i].intervalLock);
                            iIntervalLockAcquired = false;
                        }
                    }
                }
                return retval;
            }
            finally
            {
                if (globalLockAcquired)
                {
                    Monitor.Exit(globalLock);
                    globalLockAcquired = false;
                }
            }
        }

        public T FindMax()
        {
            lock (heap[0].intervalLock)
            {
                if (heap[0].lastTag == Empty)
                {
                    if (heap[0].firstTag == Empty)
                        throw new NoSuchItemException();
                    return heap[0].first;
                }
                return heap[0].last;
            }
        }

        public T FindMin()
        {
            lock (heap[0].intervalLock)
            {
                if (heap[0].firstTag == Empty)
                    throw new NoSuchItemException();
                return heap[0].first;
            }
        }

        public bool IsEmpty()
        {
            lock (heap[0].intervalLock)
            {
                if (heap[0].firstTag == Empty)
                    return true;
                return false;
            }
        }

        private bool check(int i, T min, T max)
        {
            bool retval = true;
            Interval interval = heap[i];
            T first = interval.first, last = interval.last;

            if (2 * i + 1 == size)
            {
                if (comparer.Compare(min, first) > 0)
                {
                    retval = false;
                }

                if (comparer.Compare(first, max) > 0)
                {
                    retval = false;
                }
                return retval;
            }
            else
            {
                if (comparer.Compare(min, first) > 0)
                {
                    retval = false;
                }

                if (comparer.Compare(first, last) > 0)
                {
                    retval = false;
                }

                if (comparer.Compare(last, max) > 0)
                {
                    retval = false;
                }

                int l = 2 * i + 1, r = l + 1;

                if (2 * l < size)
                    retval = retval && check(l, first, last);

                if (2 * r < size)
                    retval = retval && check(r, first, last);
            }

            return retval;
        }

        /// <summary>
        /// Increases heap size twice.
        /// </summary>
        /// <returns>an array of interval node locks</returns>
        private object[] resize()
        {
            return (object[])lockAll(() =>
            {
                Array newLocks = Array.CreateInstance(typeof(object), heap.Length * 2); //create new array of locks
                Interval[] newHeap = new Interval[heap.Length * 2]; //create new heap
                for (int y = 0; y < newHeap.Length; y++)
                {
                    if (y < heap.Length)
                    {
                        newHeap[y] = heap[y]; //add existing intervals to new heap
                    }
                    else
                    {
                        newHeap[y] = new Interval(); //fill the rest of new heap with empty intervals
                    }
                    newLocks.SetValue(newHeap[y].intervalLock, y); //fill new 
                }
                heap = newHeap;
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
                for (int i = s - 1; i >= 0; i--)
                {
                    Monitor.Exit(locks[i]);
                }
            }

            //return lockAll(0, action);
        }

        private Array lockAll(int i, Func<Array> action)
        {
            if (i >= locks.Length) //if all elements have been reached
            {
                return action(); //run given function
            }
            else
            {
                lock (locks[i])
                {
                    return lockAll(i + 1, action); //recusevley lock all nodes and pass given function
                }//unlock i'th node 
            }
        }

        private bool add(T item)
        {
            bool globalLockAcquired = false;
            bool bubbleupmax = false;
            int i = 0;

            while (true) //run until new element will be added to min or max heap
            {
                Monitor.Enter(globalLock, ref globalLockAcquired);//lock heap
                try
                {
                    if (size == heap.Length * 2)
                    {
                        locks = resize(); //resize array and assign new set of locks
                    }

                    if (size == 0)
                    {
                        lock (heap[0].intervalLock) //lock first interval node
                        {
                            size++;
                            if (globalLockAcquired)
                            {
                                Monitor.Exit(globalLock); //exit global lock
                                globalLockAcquired = false;
                            }
                            heap[0].first = item;
                            heap[0].firstTag = Available; //assign thread id to the element
                            return true;
                        }//unlock interval node
                    }

                    i = size / 2;
                    //if (size != 0) //if not the fist elment
                    //{
                    if (size % 2 == 0)
                    {
                        int p = (i + 1) / 2 - 1;
                        lock (heap[p].intervalLock) //lock parent node
                        {
                            if (heap[p].lastTag == Available)
                            {// && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId) //check if parent element is available and current elment is from current thread

                                lock (heap[i].intervalLock) //lock last node
                                {
                                    size++;
                                    if (globalLockAcquired)
                                    {
                                        Monitor.Exit(globalLock); //exit global lock
                                        globalLockAcquired = false;
                                    }
                                    heap[i].first = item;
                                    heap[i].firstTag = Thread.CurrentThread.ManagedThreadId;
                                    if (comparer.Compare(item, heap[p].last) > 0) //new element is larger than the parent's max element.
                                    {
                                        swapFirstWithLast(i, p); //swap elements and tags
                                        i = p; // assign new current node
                                        bubbleupmax = true; //new element belongs to max heap.
                                        break; //exit while
                                    }
                                    else //if (heap[p].firstTag == Available && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId)
                                    {
                                        bubbleupmax = false;
                                        break;
                                    }
                                }

                            } //unlock current node

                        } // unlock parent node

                    }
                    else
                    {
                        lock (heap[i].intervalLock)
                        {
                            if (heap[i].firstTag == Available)// && heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
                            {
                                size++;
                                if (globalLockAcquired)
                                {
                                    Monitor.Exit(globalLock); //exit global lock
                                    globalLockAcquired = false;
                                }
                                heap[i].last = item;
                                heap[i].lastTag = Thread.CurrentThread.ManagedThreadId;
                                if (comparer.Compare(heap[i].last, heap[i].first) < 0) //new element is smaller than the current min element
                                {
                                    swapFirstWithLast(i, i);
                                    bubbleupmax = false; //new element belongs to min heap
                                    break; //exit while
                                }
                                else //new element is larger than the current min element
                                {
                                    bubbleupmax = true; //new element belongs to max heap
                                    break; //exit while
                                }
                            }
                            //else if (heap[i].firstTag == Empty) //parent node is empty
                            //{
                            //    continue; //insert complete
                            //}
                        } //unlock current node
                    }
                    //  }
                }
                finally
                {
                    if (globalLockAcquired)
                    {
                        Monitor.Exit(globalLock); //exit global lock
                        globalLockAcquired = false;
                    }
                }
            }
            if (bubbleupmax) //new element belongs to max heap
            {
                bubbleUpMax(i); //bubble up new element from node i in max heap
            }
            else //new element belongs to min heap
            {
                bubbleUpMin(i); //bubble up new element from node i in min heap
            }
            return true;
        }

        //private bool add(T item)
        //{
        //    bool globalLockAcquired = false;
        //    bool bubbleupmax = false;
        //    int i = 0;

        //    Monitor.Enter(globalLock, ref globalLockAcquired);//lock heap
        //    try
        //    {
        //        if (size == heap.Length * 2)
        //        {
        //            locks = resize(); //resize array and assign new set of locks
        //        }

        //        if (size == 0)
        //        {
        //            lock (heap[0].intervalLock) //lock first interval node
        //            {
        //                size++;
        //                if (globalLockAcquired)
        //                {
        //                    Monitor.Exit(globalLock); //exit global lock
        //                    globalLockAcquired = false;
        //                }
        //                heap[0].first = item;
        //                heap[0].firstTag = Available; //assign thread id to the element
        //                return true;
        //            }//unlock interval node
        //        }

        //        i = size / 2;
        //        if (size != 0) //if not the fist elment
        //        {
        //            if (size % 2 == 0)
        //            {
        //                lock (heap[i].intervalLock)
        //                {
        //                    size++;
        //                    if (globalLockAcquired)
        //                    {
        //                        Monitor.Exit(globalLock); //exit global lock
        //                        globalLockAcquired = false;
        //                    }
        //                    heap[i].first = item;
        //                    heap[i].firstTag = Thread.CurrentThread.ManagedThreadId;
        //                }
        //                while (true) //run until new element will be added to min or max heap
        //                {
        //                    int p = (i + 1) / 2 - 1;
        //                    lock (heap[p].intervalLock) //lock parent node
        //                    {
        //                        if (heap[p].lastTag == Empty) //parent node is empty
        //                        {
        //                            break; //insert complete
        //                        }
        //                        if (heap[p].lastTag != Available)// && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId) //check if parent element is available and current elment is from current thread
        //                            continue;
        //                        lock (heap[i].intervalLock) //lock last node
        //                        {
        //                            if (comparer.Compare(heap[i].first, heap[p].last) > 0) //new element is larger than the parent's max element.
        //                            {
        //                                swapFirstWithLast(i, p); //swap elements and tags
        //                                i = p; // assign new current node
        //                                bubbleupmax = true; //new element belongs to max heap.
        //                                break; //exit while
        //                            }
        //                            else //if (heap[p].firstTag == Available && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId)
        //                            {
        //                                bubbleupmax = false;
        //                                break;
        //                            }
        //                        }
        //                    } //unlock current node
        //                } // unlock parent node

        //            }
        //            else
        //            {
        //                lock (heap[i].intervalLock)
        //                {
        //                    size++;
        //                    if (globalLockAcquired)
        //                    {
        //                        Monitor.Exit(globalLock); //exit global lock
        //                        globalLockAcquired = false;
        //                    }
        //                    heap[i].last = item;
        //                    heap[i].lastTag = Thread.CurrentThread.ManagedThreadId;
        //                }

        //                while (true)
        //                {
        //                    lock (heap[i].intervalLock)
        //                    {
        //                        if (heap[i].firstTag == Available)// && heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
        //                        {
        //                            if (comparer.Compare(heap[i].last, heap[i].first) < 0) //new element is smaller than the current min element
        //                            {
        //                                swapFirstWithLast(i, i);
        //                                bubbleupmax = false; //new element belongs to min heap
        //                                break; //exit while
        //                            }
        //                            else //new element is larger than the current min element
        //                            {
        //                                bubbleupmax = true; //new element belongs to max heap
        //                                break; //exit while
        //                            }
        //                        }
        //                        else if (heap[i].firstTag == Empty) //parent node is empty
        //                        {
        //                            break; //insert complete
        //                        }
        //                    } //unlock current node
        //                }
        //            }
        //        }
        //    }
        //    finally
        //    {
        //        if (globalLockAcquired)
        //        {
        //            Monitor.Exit(globalLock); //exit global lock
        //            globalLockAcquired = false;
        //        }
        //    }

        //    if (bubbleupmax) //new element belongs to max heap
        //    {
        //        bubbleUpMax(i); //bubble up new element from node i in max heap
        //    }
        //    else //new element belongs to min heap
        //    {
        //        bubbleUpMin(i); //bubble up new element from node i in min heap
        //    }
        //    return true;
        //}

        private void bubbleUpMax(int i)
        {
            while (i > 0) //while not root node
            {
                int p = (i + 1) / 2 - 1; //get parent node index
                lock (heap[p].intervalLock)
                {
                    lock (heap[i].intervalLock)
                    {
                        p = (i + 1) / 2 - 1;
                        if (heap[p].lastTag == Available && heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
                        {
                            if (comparer.Compare(heap[i].last, heap[p].last) > 0)
                            {
                                swapLastWithLast(i, p);
                                i = p;
                            }
                            else
                            {
                                heap[i].lastTag = Available; //mark max element available
                                return; //end bubble up
                            }
                        }
                        else if (heap[p].lastTag == Empty)
                        {
                            return; //end bubble up
                        }
                        else if (heap[i].lastTag != Thread.CurrentThread.ManagedThreadId)
                        {
                            i = p; //parent node becomes current node
                        }
                    }//unlock i
                }//unlock p
            }
            if (i == 0) //if i is root node
            {
                lock (heap[i].intervalLock)
                {
                    if (heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
                    {
                        heap[i].lastTag = Available; //mark max element available
                    }
                }//unlock i
            }
        }

        private void bubbleUpMin(int i)
        {
            while (i > 0)
            {
                int p = (i + 1) / 2 - 1;
                lock (heap[p].intervalLock)
                {
                    lock (heap[i].intervalLock)
                    {
                        p = (i + 1) / 2 - 1;
                        if (heap[p].firstTag == Available && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId)
                        {
                            if (comparer.Compare(heap[i].first, heap[p].first) < 0)
                            {
                                swapFirstWithFirst(i, p);
                                i = p;
                            }
                            else
                            {
                                heap[i].firstTag = Available;  //mark max element available
                                return; //end bubble up
                            }
                        }
                        else if (heap[p].firstTag == Empty)
                        {
                            return; // end bubble up
                        }
                        else if (heap[i].firstTag != Thread.CurrentThread.ManagedThreadId)
                        {
                            i = p;
                        }
                    }//unlock i
                }//unlock p
            }

            if (i == 0)
            {
                lock (heap[i].intervalLock)
                {
                    if (heap[i].firstTag == Thread.CurrentThread.ManagedThreadId)
                    {
                        heap[i].firstTag = Available; //mark max element available
                    }
                }//unlock i
            }
        }

        private void updateFirst(int cell, T item, int tag)
        {
            heap[cell].first = item;
            heap[cell].firstTag = tag;

        }

        private void updateLast(int cell, T item, int tag)
        {
            heap[cell].last = item;
            heap[cell].lastTag = tag;
        }

        private void swapLastWithLast(int cell1, int cell2)
        {
            T last = heap[cell2].last;
            int lastTag = heap[cell2].lastTag;
            updateLast(cell2, heap[cell1].last, heap[cell1].lastTag);
            updateLast(cell1, last, lastTag);
        }

        private void swapFirstWithLast(int cell1, int cell2)
        {
            T first = heap[cell1].first;
            int firstTag = heap[cell1].firstTag;
            updateFirst(cell1, heap[cell2].last, heap[cell2].lastTag);
            updateLast(cell2, first, firstTag);
        }

        private void swapFirstWithFirst(int cell1, int cell2)
        {
            T first = heap[cell2].first;
            int firstTag = heap[cell2].firstTag;
            updateFirst(cell2, heap[cell1].first, heap[cell1].firstTag);
            updateFirst(cell1, first, firstTag);
        }
    }
}
