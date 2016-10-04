﻿using System;
using SCG = System.Collections.Generic;
using System.Threading;

namespace C5.concurrent.heaps
{
    class HuntLockDEPQ<T> : IConcurrentPriorityQueue<T>
    {
        struct Interval
        {
            internal Node first, last;

            public override string ToString()
            {
                return string.Format("[{0}; {1}]", first.ToString(), last.ToString());
            }
        }

        struct Node
        {
            internal static object nodeLock = new object();
            internal T element;
            internal int tag;
            public override string ToString()
            {
                return string.Format("[{0}", element);
            }
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
            while (lenght < capacity)  lenght <<= 1; //lenght is always equal to 2 power by some number. 
            heap = new Interval[lenght];
        }
        

        public int Count
        {

            get
            {
                lock ()
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


        private void updateFirst(int cell, T item)
        {
            heap[cell].first.element = item;
            heap[cell].first.tag = Thread.CurrentThread.ManagedThreadId;

        }

        private void updateLast(int cell, T item)
        {
            heap[cell].last.element = item;
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
