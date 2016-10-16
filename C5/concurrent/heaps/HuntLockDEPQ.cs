using System;
using SCG = System.Collections.Generic;
using System.Threading;
using System.Runtime.CompilerServices;

namespace C5.concurrent
{
    class HuntLockDEPQ<T> : IConcurrentPriorityQueue<T>
    {
        private class Interval
        {
            internal T first = default(T), last = default(T);
            internal object intervalLock = new object();
            internal int firstTag = -1, lastTag = -1;

            public override string ToString()
            {
                return string.Format("[{0}; {1}]", first, last);
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

        public HuntLockDEPQ() : this(16) { }

        public HuntLockDEPQ(int capacity)
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
            bool globalLockAcquired = false, firstIntervalLockAcquired = false,
                iIntervalLockAcquired = false, lIntervalLockAcquired = false,
                rIntervalLockAcquired = false, lastIntervalLockAcquired = false;
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
                        retval = heap[0].first;
                        heap[0].first = default(T);
                        heap[0].firstTag = Empty;
                    }
                }
                else
                {
                    int lastcell = (size - 1) / 2;
                    Monitor.Enter(heap[0].intervalLock, ref iIntervalLockAcquired); //acquire lock of first interval node and mark it aquired.
                    try
                    {
                        firstIntervalLockAcquired = iIntervalLockAcquired;
                        lock (heap[lastcell].intervalLock)
                        {
                            retval = heap[0].last;
                            if (size % 2 == 0)
                            {
                                heap[0].last = heap[lastcell].last;
                                heap[lastcell].last = default(T);
                                heap[lastcell].lastTag = Empty;
                            }
                            else
                            {
                                heap[0].last = heap[lastcell].first;
                                heap[lastcell].first = default(T);
                                heap[lastcell].firstTag = Empty;
                            }
                            size--;
                        }

                        if (globalLockAcquired)
                        {
                            Monitor.Exit(globalLock); //release lock
                            globalLockAcquired = false; //mark global lock free
                        }

                        #region Heapify max

                        i = 0; //node index at which we satrt heapify max
                        while (true)
                        {
                            if (heap[i].lastTag != Empty && comparer.Compare(heap[i].last, heap[i].first) < 0)
                            {
                                swapFirstWithLast(i, i);
                            }

                            int currentMax = i;
                            int l = 2 * i + 1;
                            int r = l + 1;
                            bool firstMax = false;

                            try //try to obtain a lock on left child if exist
                            {
                                Monitor.Enter(heap[l].intervalLock, ref lIntervalLockAcquired);
                            }

                            catch (IndexOutOfRangeException e)
                            {
                                //ignore exception. If this is caught, it means we reached the end of heap
                            }

                            try //try to obtain a lock on righ child if exist
                            {
                                Monitor.Enter(heap[r].intervalLock, ref rIntervalLockAcquired);
                            }
                            catch (IndexOutOfRangeException e)
                            {
                                //ignore exception. If this is caught, it means we reached the end of heap
                            }
                            try
                            {
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
                                        swapFirstWithLast(currentMax, i);
                                    }
                                    else
                                    {
                                        swapLastWithLast(currentMax, i);
                                    }

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
                        if (lastIntervalLockAcquired)
                            Monitor.Exit(heap[lastcell].intervalLock);
                        if (iIntervalLockAcquired)
                            Monitor.Exit(heap[i].intervalLock);
                    }
                } //end else
                return retval;
            }
            finally
            {
                if (globalLockAcquired)
                    Monitor.Exit(globalLock); //exit global lock
            }
        }

        public T DeleteMin()
        {
            throw new NotImplementedException();
            //int bottom;
            //int i = 0;
            //int localSize;
            //var retval = default(T);

            ////Hunt: grab item from bottom to replace to-be-delated top item
            //lock (globalLock)
            //{
            //    if (size == 0)
            //    {
            //        throw new NoSuchItemException();
            //    }
            //    size--;
            //    localSize = size;
            //    bottom = (size / 2);

            //    lock (heap[bottom].intervalLock)
            //    {
            //        //default return, top would have the highest priority (e.g. lowest number), bottom would have the lowest. (Min-heap)

            //        retval = heap[bottom].first;
            //        heap[bottom].firstTag = Empty;
            //    }

            //    lock (heap[i].intervalLock)
            //    {
            //        //Hunt: Lock first item stop if only item in the heap
            //        if (heap[i].firstTag == Empty)
            //        {
            //            return retval;
            //        }

            //        //replace the top item with the "bottom" or last element and mark it for deletion
            //        swapFirstWithFirst(i, bottom);
            //        heap[i].firstTag = Available;

            //        //check that the new top min element (first) isent greater then the top Max(last) element. (vice-versa for delete-max)
            //        //T min = heap[i].first;
            //        //int minTag = heap[i].firstTag;
            //        //T max = heap[i].last;
            //        //int maxTag = heap[i].lastTag;
            //        //check if new min is greater then max, and if it is, swap them
            //        if (comparer.Compare(heap[i].first, heap[i].last) > 0)
            //        {
            //            swapFirstWithLast(i, i);
            //            //updateFirst(i, max, maxTag);
            //            //updateLast(i, min, minTag);
            //        }
            //    }
            //    // HuntHeapify
            //    // i == 0 aka first element in the heap
            //    while (i < localSize / 2)
            //    {
            //        var left = i * 2 + 1;
            //        var right = i * 2 + 2;
            //        int child;

            //        lock (heap[left].intervalLock)
            //        {
            //            lock (heap[right].intervalLock)
            //            {
            //                if (heap[left].firstTag.Equals(Empty))
            //                {
            //                    break;
            //                }
            //                //else if (heap[right].firstTag.Equals(Empty) || comparer.Compare(heap[left].first, heap[right].first) < 0)
            //                //hunt uses the Empty tag to check for empty nodes, we cant quite do the same as we dont actually have a node.

            //                else if (left * 2 < localSize && comparer.Compare(heap[left].first, heap[i].first) < 0)
            //                {
            //                    child = left;
            //                }
            //                else
            //                {
            //                    child = right;
            //                }

            //                //if child has higer priority (lower) then parent then swap, if not then stop
            //                if (comparer.Compare(heap[child].first, heap[i].first) < 0)
            //                {
            //                    swapFirstWithFirst(child, i);
            //                    i = child;

            //                }
            //                else
            //                {
            //                    break;
            //                }
            //            }

            //        }
            //    }//end huntHeapify
            //}//unlock i

            //return retval;
        }

        public T FindMax()
        {
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();
                return heap[0].last;
            } //unlock heap
        }

        public T FindMin()
        {
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();
                return heap[0].first;
            } //unlock heap
        }

        public bool IsEmpty()
        {
            lock (globalLock)
            {
                if (size == 0)
                    return true;
                return false;
            } //unlock heap
        }

        private bool check(int i, T min, T max)
        {
            bool retval = true;
            Interval interval = heap[i];
            T first = interval.first, last = interval.last;
            int firsttag = interval.firstTag, lasttag = interval.lastTag;

            if (firsttag != Available || lasttag != Available) //check if the tags of the given node are marked Available. 
            {
                return false; //some tags are not marked Available.
            }
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
            return lockAll(0, action);
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
            int s = 0;
            bool bubbleupmax = false;
            int i = 0;

            lock (globalLock)//lock heap
            {
                s = size; // get size
                size++; //increment size
                i = s / 2;

                if (s == heap.Length * 2)
                {
                    locks = resize(); //resize array and assign new set of locks
                }

                if (s == 0)
                {
                    lock (heap[0].intervalLock) //lock first interval node
                    {
                        heap[0].first = item;
                        heap[0].firstTag = Thread.CurrentThread.ManagedThreadId; //assign thread id to the element
                    }//unlock interval node
                }

                if (s != 0) //if not the fist elment
                {
                    if (s % 2 == 0)
                    {
                        int p = (i + 1) / 2 - 1;
                        lock (heap[p].intervalLock) //lock parent node
                        {
                            lock (heap[i].intervalLock) //lock last interval node and insert new item
                            {
                                heap[i].first = item;
                                heap[i].firstTag = Thread.CurrentThread.ManagedThreadId;
                            }
                        } //unlock current node

                        while (true) //run until new element will be added to min or max heap
                        {
                            lock (heap[p].intervalLock) //lock parent node
                            {
                                lock (heap[i].intervalLock) //lock last node
                                {
                                    if (heap[p].lastTag == Available && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId) //check if parent element is available and current elment is from current thread
                                    {
                                        if (comparer.Compare(heap[i].first, heap[p].last) > 0) //new element is larger than the parent's max element.
                                        {
                                            swapFirstWithLast(i, p); //swap elements and tags
                                            i = p; // assign new current node
                                            bubbleupmax = true; //new element belongs to max heap.
                                            break; //exit while
                                        }
                                        else if (heap[p].firstTag == Available && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId)
                                        {
                                            if (comparer.Compare(heap[i].last, heap[p].first) < 0) //new element is smaller than the parent's min element
                                            {
                                                bubbleupmax = false; //new element belongs to min heap
                                                break; //exit while
                                            }
                                            else //new element is larger than parent's min element
                                            {
                                                heap[i].firstTag = Available;
                                                return true; //insert complete
                                            }
                                        }
                                        else if (heap[p].firstTag == Empty) //parent node is empty
                                        {
                                            return true; //insert complete
                                        }
                                    }
                                    else if (heap[p].lastTag == Empty) //parent node is empty 
                                    {
                                        return true; //insert complete
                                    }
                                } //unlock current node
                            } // unlock parent node
                        } //while loop
                    }
                    else
                    {
                        lock (heap[i].intervalLock) //lock last interval node and insert new item
                        {
                            heap[i].last = item;
                            heap[i].lastTag = Thread.CurrentThread.ManagedThreadId;
                        } //unlock current node

                        while (true) //run until new element will be added to min or max heap
                        {
                            lock (heap[i].intervalLock)
                            {
                                if (heap[i].firstTag == Available && heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
                                {
                                    if (comparer.Compare(heap[i].last, heap[i].first) < 0) //new element is smaller than the current min element
                                    {
                                        swapFirstWithLast(i, i); //swap elements
                                        bubbleupmax = false; //new element belongs to min heap
                                        break; //exit while
                                    }
                                    else //new element is larger than the current min element
                                    {
                                        bubbleupmax = true; //new element belongs to max heap
                                        break; //exit while
                                    }
                                }
                                else if (heap[i].firstTag == Empty) //parent node is empty
                                {
                                    return true; //insert complete
                                }
                            } //unlock current node
                        } //while loop
                    }
                }
            } //unlock heap

            if (s == 0) //new element is the first element in the heap
            {
                bubbleUpMin(i); //bubble up min heap. Since i = 0, it will only assing available.
                return true;
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
