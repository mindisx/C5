using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;

namespace C5.concurrent
{
    [Serializable]
    public class GlobalLockDEPQ<T> : IConcurrentPriorityQueue<T>
    {
        #region fields
        struct Interval
        {
            internal T first, last;

            public override string ToString()
            {
                return string.Format("[{0}; {1}]", first, last);
            }
        }
        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        Interval[] heap;
        int size;
        #endregion

        /// <summary>
        /// Default GlobalLockDEPQ is size 16
        /// </summary>
        public GlobalLockDEPQ() : this(16) { }
        
        GlobalLockDEPQ(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            heap = new Interval[capacity + 1];
        }
        
        public int Count
        {
            get { return size; } 
        }

        public bool Add(T item)
        {
            throw new NotImplementedException();

        }

        public SCG.IEnumerable<T> All()
        {
            
            T[] al = new T[size];
            for (int i = 0; i < size; i++)
            {
                if (i%2 == 0)
                {
                    al[i] = heap[i].first;
                }
                else
                {
                    al[i] = heap[i].last;
                }

            }
            return al;

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
            if (size == 0)
                throw new NoSuchItemException();

            return heap[1].last;
        }

        public T FindMin()
        {
            if (size == 0)
                throw new NoSuchItemException();

            return heap[1].first;
        }

        public bool IsEmpty()
        {
            if (size == 0){ return true;}
            return false;
        }
    }
}
