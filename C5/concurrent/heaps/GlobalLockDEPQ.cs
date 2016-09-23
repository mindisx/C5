using System;
using System.Collections.Generic;
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
            public override string ToString() { return string.Format("[{0}; {1}]", first, last); }
        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemequalityComparer;

        Interval[] heap;
        int size;
        #endregion

        public GlobalLockDEPQ(int capacity)
        {
            
        }
        
        public int Count
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public bool Add(T item)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<T> All()
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
            if (heap.Length == 0)
            throw new Excep();

            return heap[1].first;
        }

        public T FindMin()
        {
            throw new NotImplementedException();
        }

        public bool IsEmpty()
        {
            throw new NotImplementedException();
        }
    }
}
