using System;
using SCG = System.Collections.Generic;
using System.Threading;

namespace C5.concurrent.heaps
{
    class HuntLockDEPQ<T> : IConcurrentPriorityQueue<T>
    {
        #region fields
        private struct Interval
        {
            internal Node first, last;
            internal static object intervalLock = new object();

            public object Lock
            {
                get
                {
                    return intervalLock;
                }
            }

            

            public override string ToString()
            {
                return string.Format("[{0}; {1}]", first.ToString(), last.ToString());
            }
        }

        private struct Node
        {
            internal static object nodeLock = new object();
            public object Lock
            {
                get
                {
                    return nodeLock;
                }
            }
            internal T element;
            // -1 avalible -2 empty
            internal int tag;
            public override string ToString()
            {
                return string.Format("[{0}", element);
            }
        }

        private static object globalLock = new object();

        //private T lastCell;

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        Interval[] heap;
        volatile int size;


        private int Avalible
        {
            get { return -2; }
        }

        private int Empty 
        {
            get { return -1; }

        }

        #endregion

        public HuntLockDEPQ() : this(16) { }

        public HuntLockDEPQ(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            int lenght = 1;
            while (lenght < capacity)  lenght <<= 1; //lenght is always equal to 2 power by some number. 
            heap = new Interval[lenght];
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
            throw new NotImplementedException();
        }

        public SCG.IEnumerable<T> All()
        {
            throw new NotImplementedException();
        }

        public bool Check()
        {
            throw new NotImplementedException();
        }
        x
        private bool check(T min, T max)
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

        public T DeleteMax()
        {
            throw new NotImplementedException();
        }

        public T DeleteMin()
        {
            int lastCell;
            int localSize = 0;
            int i = 0;

            //Hunt: grab item from bottom to replace to-be-delated top item
            lock (globalLock)
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }
                localSize = size;
                size--;
                lastCell = (localSize-1)/2;
                
            }

            lock (heap[lastCell].Lock)
            {
                lock (heap[lastCell].first.Lock)
                {
                    heap[lastCell].first.tag = Empty;
                }
                
            }

            
            //Hunt: Lock first item stop if only iteam in the heap
            lock (globalLock)
            {
                if (size == 1)
                {
                    size = 0;
                    lock (heap[0].Lock)
                    {
                        lock (heap[0].first.Lock)
                        {
                            return heap[0].first.element;
                        }
                    }                                    
                }
            }

            lock (heap[0].Lock)
            {
                lock (heap[0].first.Lock)
                {
                    //replace the top item with the "bottom" or last element
                    updateFirst(0, heap[lastCell].first.element);
                    heap[0].first.tag = Avalible;

                    //check that the new top min element (first) isent greater then the top Max(last) element. (vice-versa for delete-max)
                    lock (heap[0].last.Lock)
                    {
                        T min = heap[0].first.element;
                        int minTag = heap[0].first.tag;
                        T max = heap[0].last.element;
                        int maxTag = heap[0].last.tag;
                    }
                   
                    
                    
                    if (comparer.Compare())
                    {
                        var tempFirst = heap[0].first;
                        var tempLast = heap[0].last;
                        heap[0].first = tempLast;
                        heap[0].last = tempFirst;
                    }
                }        
              
            }

            //heapify
            // i == 0 aka first element in the heap
            while (i < localSize/2)
            {
                var left = (i + 1)*2;
            }


            return default(T);


        }


       
        

        public T FindMax()
        {
            throw new NotImplementedException();
        }

        public T FindMin()
        {
            throw new NotImplementedException();
        }

        public bool IsEmpty()
        {
            throw new NotImplementedException();
        }


        private void updateFirst(int cell, T item)
        {
            heap[cell].first.element = item;
            //sets the pid
            heap[cell].first.tag = Thread.CurrentThread.ManagedThreadId;

        }

        private void updateLast(int cell, T item)
        {
            heap[cell].last.element = item;
            //sets the pid
            heap[cell].last.tag = Thread.CurrentThread.ManagedThreadId;
        }

        private void swapLastWithLast(int cell1, int cell2)
        {
            T last = heap[cell2].last.element;
            updateLast(cell2, heap[cell1].last.element);
            updateLast(cell1, last);
        }

        private void swapFirstWithLast(int cell1, int cell2)
        {
            T first = heap[cell1].first.element;
            updateFirst(cell1, heap[cell2].last.element);
            updateLast(cell2, first);
        }

        private void swapFirstWithFirst(int cell1, int cell2)
        {
            T first = heap[cell2].first.element;
            updateFirst(cell2, heap[cell1].first.element);
            updateFirst(cell1, first);
        }
    }
}
