using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace C5.concurrent
{
    [Serializable]
    public class GlobalLockDEPQ<T> : IConcurrentPriorityQueue<T>
    {
        struct Interval
        {
            internal T first, last;
        }

        Interval[] heap;
        int size;


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
    }
}
