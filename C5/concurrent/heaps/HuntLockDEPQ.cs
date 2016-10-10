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
            get { return -1; }
        }

        private int Empty 
        {
            get { return -2; }

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

        public T DeleteMax()
        {
            throw new NotImplementedException();
        }

        public T DeleteMin()
        {
            Interval lastCell;
            //Hunt: grab item from bottom to replace to-be-delated top item
            lock (globalLock)
            {
                if (Count == 0)
                {
                    throw new NoSuchItemException();
                }
                //re-entrant lock on count
                lastCell = heap[Count/2];

            }
            lock (lastCell.Lock)
            {
                lastCell.first.tag = Empty;
                lastCell.last.tag = Empty;
            }

            
            //Hunt: Lock first item stop if only iteam in the heap
            lock (heap[0].first.Lock)
            {
                if (size == 1)
                {
                    size = 0;
                    return heap[0].first.element;
                }
                //replace tio item with item at the bottom
                updateFirst(0, lastCell.first.element);
                heap[0].first.tag = Avalible;

            }

            int i = 0;
            size--;
            while (i < size / 2)
            {
                
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
