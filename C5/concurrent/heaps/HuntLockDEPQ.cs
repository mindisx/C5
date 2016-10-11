using System;
using SCG = System.Collections.Generic;
using System.Threading;

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

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        Interval[] heap;
        int size;

        public HuntLockDEPQ() : this(16) { }

        public HuntLockDEPQ(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            int lenght = 1;
            while (lenght < capacity) lenght <<= 1;
            heap = new Interval[lenght];
            for (int i = 0; i < lenght; i++)
            {
                Interval interval = new Interval();
                interval.firstTag = -2;
                interval.lastTag = -2;
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
            throw new NotImplementedException();
        }

        public T DeleteMin()
        {
            throw new NotImplementedException();
        }

        public T FindMax()
        {
            return heap[0].last;
        }

        public T FindMin()
        {
            return heap[0].first;
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

                if (s > heap.Length)
                {
                    Interval[] tempheap = new Interval[heap.Length * 2];
                    for (int y = 0; y < tempheap.Length; y++)
                    {
                        if (y < heap.Length)
                        {
                            tempheap[y] = heap[y];
                        }
                        else
                        {
                            Node first = new Node();
                            first.element = default(T);
                            first.tag = -1;
                            Node last = new Node();
                            last.element = default(T);
                            last.tag = -1;

                            Interval interval = new Interval();
                            interval.first = first;
                            interval.last = last;

                            tempheap[y] = interval;
                        }
                    }
                    heap = tempheap;
                }
                if (s == 0)
                {
                    lock (heap[i].intervalLock)//lock last interval node
                    {
                        Node node = new Node();
                        lock (node.nodeLock)
                        {
                            node.element = item;
                            node.tag = Thread.CurrentThread.ManagedThreadId;
                            heap[0].first = node;
                        }//unlock node
                    }//unlock interval node
                }

                if (s != 0)
                {
                    if (s % 2 == 0)
                    {
                        lock (heap[i].intervalLock)//lock last interval node
                        {
                            Node node = new Node();
                            lock (node.nodeLock)
                            {
                                node.element = item;
                                node.tag = Thread.CurrentThread.ManagedThreadId;
                                heap[i].first = node;
                            }//unlock node
                        }//unlock interval node
                    }
                    else
                    {
                        lock (heap[i].intervalLock)//lock last interval node
                        {
                            Node node = new Node();
                            lock (node.nodeLock)
                            {
                                node.element = item;
                                node.tag = Thread.CurrentThread.ManagedThreadId;
                                heap[i].last = node;
                            }//unlock node
                        }//unlock interval 
                    }
                }

            } //unlock heap

            if (s != 0) //not the first element
            {
                if (s % 2 == 0)
                {
                    int p = (i + 1) / 2 - 1;
                    lock (heap[p].last.nodeLock) //lock last interval node
                    {
                        lock (heap[i].first.nodeLock)
                        {
                            if (heap[p].last.tag == -2 && heap[i].first.tag == Thread.CurrentThread.ManagedThreadId)
                            {
                                if (comparer.Compare(heap[i].first.element, heap[p].last.element) > 0)  //new element is smaller than current min element.
                                {
                                    swapFirstWithLast(i, p);
                                    i = p;
                                    bubbleupmax = true;
                                }
                            }
                            else if (heap[p].last.tag == -1)
                            {
                                return true;
                            }
                            else if (heap[i].first.tag != Thread.CurrentThread.ManagedThreadId)
                            {
                                bubbleupmax = false;
                                i = p;
                            }
                        }//unlock node
                    } //unlock parent max heap node
                }
                else
                {

                    lock (heap[i].first.nodeLock)
                    {
                        lock (heap[i].last.nodeLock) //lock last interval node
                        {
                            if (heap[i].first.tag == -2 && heap[i].last.tag == Thread.CurrentThread.ManagedThreadId)
                            {
                                if (comparer.Compare(heap[i].last.element, heap[i].first.element) < 0)
                                {
                                    swapFirstWithLast(i, i);
                                    bubbleupmax = false;
                                }
                                else
                                {
                                    bubbleupmax = true;
                                }
                            }
                            else if (heap[i].first.tag == -1)
                            {
                                return true;
                            }
                            else if (heap[i].last.tag != Thread.CurrentThread.ManagedThreadId)
                            {
                                bubbleupmax = false;
                            }
                        }//unlock node
                    } //unlock sibling min node
                }
            }

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

        private void bubbleUpMax(int i)
        {

            while (i > 0)
            {
                int p = (i + 1) / 2 - 1;
                lock (heap[p].last.nodeLock)
                {
                    lock (heap[i].last.nodeLock)
                    {
                        T max = heap[i].last.element;
                        int maxTag = heap[i].last.tag;
                        T iv = max;
                        p = (i + 1) / 2 - 1;
                        if (heap[p].last.tag == -2 && heap[i].last.tag == Thread.CurrentThread.ManagedThreadId)
                        {
                            if (comparer.Compare(iv, max = heap[p].last.element) > 0)
                            {
                                swapLastWithLast(i, p);
                                max = iv;
                                i = p;
                            }
                            else
                            {
                                heap[i].last.tag = -2; //available
                                return;
                            }
                        }
                        else if (heap[p].last.tag == -1)
                        {
                            return;
                        }
                        else if (heap[i].last.tag != Thread.CurrentThread.ManagedThreadId)
                        {
                            i = p;
                        }
                    }//unlock i
                }//unlock p

            }
            if (i == 0)
            {
                lock (heap[i].last.nodeLock)
                {
                    if (heap[i].last.tag == Thread.CurrentThread.ManagedThreadId)
                    {
                        heap[i].last.tag = -2; //available
                    }
                }//unlock i
            }
        }

        private void bubbleUpMin(int i)
        {
            while (i > 0)
            {
                int p = (i + 1) / 2 - 1;
                lock (heap[p].first.nodeLock)
                {
                    lock (heap[i].first.nodeLock)
                    {
                        T min = heap[i].first.element;
                        int minTag = heap[i].first.tag;
                        T iv = min;
                        p = (i + 1) / 2 - 1;
                        if (heap[p].first.tag == -2 && heap[i].first.tag == Thread.CurrentThread.ManagedThreadId)
                        {
                            if (comparer.Compare(iv, min = heap[p].first.element) < 0)
                            {
                                swapFirstWithFirst(i, p);
                                min = iv;
                                i = p;
                            }
                            else
                            {
                                heap[i].first.tag = -2; //available
                                return;
                            }
                        }
                        else if (heap[p].first.tag == -1)
                        {
                            return;
                        }
                        else if (heap[i].first.tag != Thread.CurrentThread.ManagedThreadId)
                        {
                            i = p;
                        }
                    }//unlock i
                }//unlock p
            }

            if (i == 0)
            {
                lock (heap[i].first.nodeLock)
                {
                    if (heap[i].first.tag == Thread.CurrentThread.ManagedThreadId)
                    {
                        heap[i].first.tag = -2; //available
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
