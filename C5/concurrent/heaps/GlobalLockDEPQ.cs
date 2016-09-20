using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace C5.concurrent.heaps
{
    class GlobalLockDEPQ<T> : IConcurrentPriorityQueue<T>
    {
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
