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
            internal T first, last;
            internal object intervalLock = new object();
            internal int firstTag, lastTag;

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
        /// <summary>
        /// Used for resizing. 
        /// </summary>
        /// <param name="action"></param>
        /// <returns>A resized lock object list</returns>
        private Array lockAll(Func<Array> action)
        {
            return lockAll(0, action);
        }
        private Array lockAll(int i, Func<Array> action)
        {
            if (i >= locks.Length)
            {
                return action();
            }
            else
            {
                lock (locks[i])
                {
                    return lockAll(i + 1, action);
                }
            }
        }

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
                Interval interval = new Interval();
                interval.firstTag = -2;
                interval.lastTag = -2;
                locks[i] = interval.intervalLock;
                heap[i] = interval;
            }
        }


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

        public bool Add(T item)
        {
            if (add(item))
                return true;
            return false;
        }

        private bool add(T item)
        {
            int s = 0;
            bool bubbleupmax = false;
            int i = 0;
            /* add new node to an end of the heap */
            lock (globalLock)//lock heap
            {
                s = size; // get size
                size++; //increment size
                i = s / 2;

                if (s == heap.Length * 2)
                {
                    locks = resize();
                }
                if (s == 0)
                {
                    lock (heap[0].intervalLock)//lock last interval node
                    {
                        heap[0].first = item;
                        heap[0].firstTag = Thread.CurrentThread.ManagedThreadId;
                    }//unlock interval node
                }

                if (s != 0)
                {
                    if (s % 2 == 0)
                    {
                        int p = (i + 1) / 2 - 1;
                        lock (heap[p].intervalLock)
                        {
                            lock (heap[i].intervalLock)//lock last interval node
                            {
                                heap[i].first = item;
                                heap[i].firstTag = Thread.CurrentThread.ManagedThreadId;
                            }
                        }

                        while (true)
                        {
                            lock (heap[p].intervalLock)
                            {
                                lock (heap[i].intervalLock)
                                {
                                    if (heap[p].lastTag == Available && heap[i].firstTag == Thread.CurrentThread.ManagedThreadId)
                                    {
                                        if (comparer.Compare(heap[i].first, heap[p].last) > 0)  //new element is smaller than current min element.
                                        {
                                            swapFirstWithLast(i, p);
                                            i = p;
                                            bubbleupmax = true;
                                        }
                                        else
                                        {
                                            if (comparer.Compare(heap[i].last, heap[p].first) < 0)  //parent's min element is smaller than new element. 
                                            {
                                                bubbleupmax = false;
                                            }
                                            else
                                            {
                                                heap[i].firstTag = Available;
                                                return true;
                                            }
                                        }
                                        break;
                                    }
                                    else if (heap[p].lastTag == Empty)
                                    {
                                        return true;
                                    }
                                }
                            }//unlock interval node
                        }
                    }
                    else
                    {
                        lock (heap[i].intervalLock)//lock last interval node
                        {
                            heap[i].last = item;
                            heap[i].lastTag = Thread.CurrentThread.ManagedThreadId;
                        }

                        while (true)
                        {
                            lock (heap[i].intervalLock)
                            {
                                if (heap[i].firstTag == Available && heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
                                {
                                    if (comparer.Compare(heap[i].last, heap[i].first) < 0)
                                    {
                                        swapFirstWithLast(i, i);
                                        bubbleupmax = false;
                                    }
                                    else
                                    {
                                        bubbleupmax = true;
                                    }
                                    break;
                                }
                                else if (heap[i].firstTag == Empty)
                                {
                                    return true;
                                }
                            }
                        }//unlock interval 
                    }
                }
            } //unlock heap

            if (s == 0)
            {
                bubbleUpMin(i);
                return true;
            }

            if (bubbleupmax)
            {
                bubbleUpMax(i);
            }
            else
            {
                bubbleUpMin(i);
            }
            return true;
        }

        /// <summary>
        /// Returns a list of all elements not necessary in actual priority
        /// </summary>
        /// <returns></returns>
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
                            elements[counter] = heap[i / 2].first;
                            counter++;
                        }
                        else
                        {
                            elements[counter] = heap[i / 2].last;
                            counter++;
                        }
                    }
                    return elements;
                });
            }
        }

        public bool Check()
        {
            lock (globalLock)
            {
                if (size == 0)
                    return true;

                if (size == 1)
                    return (object)(heap[0].first) != null;

                return check(0, heap[0].first, heap[0].last);
            }
        }

        private bool check(int i, T min, T max)
        {
            bool retval = true;
            Interval interval = heap[i];
            T first = interval.first, last = interval.last;
            int firsttag = interval.firstTag, lasttag = interval.lastTag;

            if (firsttag != Available || lasttag != Available)
            {
                return false;
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

        public T DeleteMax()
        {
            throw new NotImplementedException();
        }

        public T DeleteMin()
        {
            int bottom;
            int top = 0;
            int localSize;
            int i = 0;
            var retval = default(T);

            //Hunt: grab item from bottom to replace to-be-delated top item
            lock (globalLock)
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }

                size--;
                localSize = size;
                bottom = localSize / 2;
            }

            lock (heap[bottom].intervalLock)
            {
                //default return
                retval = heap[bottom].first;
                heap[bottom].firstTag = Empty;
            }


            lock (globalLock)
            {
                lock (heap[top].intervalLock)
                {
                    //Hunt: Lock first item stop if only iteam in the heap
                    if (heap[top].firstTag == Empty)
                    {
                        return retval;
                    }

                    //replace the top item with the "bottom" or last element and mark it for deletion
                    updateFirst(top, heap[bottom].first, heap[bottom].firstTag);
                    heap[top].firstTag = Available;

                    //check that the new top min element (first) isent greater then the top Max(last) element. (vice-versa for delete-max)
                    T min = heap[top].first;
                    int minTag = heap[top].firstTag;
                    T max = heap[top].last;
                    int maxTag = heap[top].lastTag;
                    //check if new min is greater then max, and if it is, swap them
                    if (comparer.Compare(min, max) > 0)
                    {
                        updateFirst(top, max, maxTag);
                        updateLast(top, min, minTag);
                    }
                }
            }



            //heapify
            // i == 0 aka first element in the heap
            while (i < localSize / 2)
            {
                var left = heap[(i + 1) * 2]; var right = heap[(i + 1) * 2 + 1];

                lock (left.intervalLock)
                {
                    lock (right.intervalLock)
                    {
                        if (left.firstTag.Equals(Empty))
                        {
                            break;
                        }
                        else if (right.firstTag.Equals(Empty))
                        {

                        }
                    }
                    var child = right;

                }

            }

            return default(T);



        }

        public T FindMax()
        {
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();
                return heap[0].last;
            }
        }

        public T FindMin()
        {
            lock (globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();
                return heap[0].first;
            }
        }

        public bool IsEmpty()
        {
            lock (globalLock)
            {
                if (size == 0)
                    return true;
                return false;
            }
        }
                

        private object[] resize()
        {
            return (object[])lockAll(() =>
            {
                Array newLocks = Array.CreateInstance(typeof(object), heap.Length * 2);
                Interval[] newHeap = new Interval[heap.Length * 2];
                for (int y = 0; y < newHeap.Length; y++)
                {
                    if (y < heap.Length)
                    {
                        newHeap[y] = heap[y];
                    }
                    else
                    {
                        Interval interval = new Interval();
                        interval.firstTag = -2;
                        interval.lastTag = -2;
                        newHeap[y] = interval;
                    }
                    newLocks.SetValue(newHeap[y].intervalLock, y);
                }
                heap = newHeap;
                return newLocks;
            });
        }
                  

        private void bubbleUpMax(int i)
        {
            while (i > 0)
            {
                int p = (i + 1) / 2 - 1;
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
                                heap[i].lastTag = Available; //available
                                return;
                            }
                        }
                        else if (heap[p].lastTag == Empty)
                        {
                            return;
                        }
                        else if (heap[i].lastTag != Thread.CurrentThread.ManagedThreadId)
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
                    if (heap[i].lastTag == Thread.CurrentThread.ManagedThreadId)
                    {
                        heap[i].lastTag = Available; //available
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
                                heap[i].firstTag = Available; //available
                                return;
                            }
                        }
                        else if (heap[p].firstTag == Empty)
                        {
                            return;
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
                        heap[i].firstTag = Available; //available
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
