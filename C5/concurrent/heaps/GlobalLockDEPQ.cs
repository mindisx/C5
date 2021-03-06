﻿using System;
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
        private static readonly Object globalLock = new Object();
        #endregion

        /// <summary>
        /// Default GlobalLockDEPQ is size 16
        /// </summary>
        public GlobalLockDEPQ() : this(16) { }

        public GlobalLockDEPQ(int capacity)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            int lenght = 1;
            while (lenght < capacity) lenght <<= 1; //lenght is always equal to 2 power by some number. 
            heap = new Interval[lenght];
        }

        public int Count
        {
            get
            {
                lock(globalLock)
                {
                    return size;
                }
            }
        }

        public bool Add(T item)
        {
            lock(globalLock)
            {
                if (add(item))
                    return true;
                return false;
            }
        }

        public SCG.IEnumerable<T> All()
        {
            lock(globalLock)
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }

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
            }
        }

        public bool Check()
        {
            lock(globalLock)
            {
                if (size == 0)
                    return true;

                if (size == 1)
                    return (object)(heap[0].first) != null;

                return check(0, heap[0].first, heap[0].last);
            }
        }

        public T DeleteMax()
        {
            lock(globalLock)
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }

                T retval;
                if (size == 1) //if there is only one element in the heap, assign it as a return value.
                {
                    retval = heap[0].first;
                    heap[0].first = default(T);
                    size = 0;
                }
                else
                {
                    retval = heap[0].last;
                    int lastcell = (size - 1) / 2;
                    if (size % 2 == 0)
                    {
                        updateLast(0, heap[lastcell].last);
                        heap[lastcell].last = default(T);
                    }
                    else
                    {
                        updateLast(0, heap[lastcell].first);
                        heap[lastcell].first = default(T);
                    }
                    size--;
                    heapifyMax(0);
                }
                return retval;
            }
        }

        public T DeleteMin()
        {
            lock(globalLock)
            {
                if (size == 0)
                {
                    throw new NoSuchItemException();
                }

                T retval = heap[0].first;
                if (size == 1)
                {
                    size = 0;
                    heap[0].first = default(T);
                }
                else
                {
                    int lastcell = (size - 1) / 2;
                    if (size % 2 == 0)
                    {
                        updateFirst(0, heap[lastcell].last); //take last element in a heap and put as a root min. 
                        heap[lastcell].last = default(T);
                    }
                    else
                    {
                        updateFirst(0, heap[lastcell].first);
                        heap[lastcell].first = default(T);
                    }
                    size--;
                    heapifyMin(0);
                }
                return retval;
            }
        }

        public T FindMax()
        {
            lock(globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();

                return heap[0].last;
            }
        }

        public T FindMin()
        {
            lock(globalLock)
            {
                if (size == 0)
                    throw new NoSuchItemException();

                return heap[0].first;
            }
        }

        public bool IsEmpty()
        {
            lock(globalLock)
            {
                if (size == 0) { return true; }
                return false;
            }
        }

        private bool add(T item)
        {

            if (size == 0)
            {
                heap[0].first = item;
                size = 1;
                return true;
            }
            if (size == 2 * heap.Length)    //heap is full. Double its size 
            {
                Interval[] tempheap = new Interval[heap.Length * 2];
                for (int i = 0; i < heap.Length; i++)
                    tempheap[i] = heap[i];
                heap = tempheap;
            }

            if (size % 2 == 0)  //all used nodes has both, min and max element assigned. 
            {
                int i = size / 2;   //since heap starts from node in position 0, i is always next empty node.
                int p = (i + 1) / 2 - 1;    //parent node. 
                T tmp = heap[p].last;   //parent max element
                if (comparer.Compare(item, tmp) > 0)    // new item is greather than parent max element. 
                {
                    updateFirst(i, tmp);    //move parent's max element to child node. 
                    updateLast(p, item);    //update parent max element to new element. 
                    bubbleUpMax(p); //TODO: bubble up max from p node. 
                }
                else    //new item is smaller or equal to parent's max element
                {
                    updateFirst(i, item);   //add new element to next empty node as min element.
                    if (comparer.Compare(item, heap[p].first) < 0)  //parent's min element is smaller than new element. 
                    {
                        bubbleUpMin(i); //TODO: bubble up min from i node
                    }
                }
            }
            else
            {
                int i = size / 2;
                T other = heap[i].first;    //get last used node's min element
                if (comparer.Compare(item, other) < 0)  //new element is smaller than current min element.
                {
                    updateLast(i, other);   //move node's min element to be max element.
                    updateFirst(i, item);   //put new element as new min elmenet.
                    bubbleUpMin(i);//TODO: bubble up min element from node i.
                }
                else    //new element is larger than node's min element.
                {
                    updateLast(i, item);
                    bubbleUpMax(i); //TODO: bubble up max element from node i.
                }
            }
            size++;
            return true;


        }

        private void bubbleUpMax(int i)
        {
            if (i > 0)
            {
                T max = heap[i].last;
                T iv = max;
                int p = (i + 1) / 2 - 1;
                while (i > 0)
                {
                    if (comparer.Compare(iv, max = heap[p = (i + 1) / 2 - 1].last) > 0)
                    {
                        updateLast(i, max);
                        max = iv;
                        i = p;
                    }
                    else
                    {
                        break;
                    }
                }
                updateLast(i, iv);
            }
        }

        private void bubbleUpMin(int i)
        {
            if (i > 0) //if node is not root node
            {
                T min = heap[i].first; //get min element
                T iv = min; //get aux min element
                int p = (i + 1) / 2 - 1;    //get parent node index

                while (i > 0)    //while i is not root index
                {
                    if (comparer.Compare(iv, min = heap[p = (i + 1) / 2 - 1].first) < 0) //if i min element is smaller than parent min element
                    {
                        updateFirst(i, min);    //push parents min element to child node. 
                        min = iv; //assign new min element from child node.
                        i = p; //move up the tree. paernt becomes child. 
                    }
                    else
                    {
                        break;
                    }
                }
                updateFirst(i, iv); //last update. Put min element in a current node. 
            }
        }

        private void updateLast(int cell, T item)
        {
            heap[cell].last = item;
        }

        private void updateFirst(int cell, T item)
        {
            heap[cell].first = item;

        }

        private bool check(int i, T min, T max)
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

        private bool heapifyMax(int cell)
        {
            bool swappedroot = false;
            if (2 * cell + 1 < size && comparer.Compare(heap[cell].last, heap[cell].first) < 0)
            {
                swappedroot = true;
                swapFirstWithLast(cell, cell);
            }

            int currentMax = cell;
            int l = 2 * cell + 1;
            int r = l + 1;
            bool firstMax = false;
            if (2 * l + 1 < size) //left child first and last exist
            {
                if (comparer.Compare(heap[l].last, heap[currentMax].last) > 0) //left child max element is greather 
                    currentMax = l;
            }
            else if (2 * l + 1 == size)//only left childs min element exist
            {
                if (comparer.Compare(heap[l].first, heap[currentMax].last) > 0)
                {
                    currentMax = l;
                    firstMax = true;
                }
            }

            if (2 * r + 1 < size)
            {
                if (comparer.Compare(heap[r].last, heap[currentMax].last) > 0)
                    currentMax = r;
            }
            else if (2 * r + 1 == size)
            {
                if (comparer.Compare(heap[r].first, heap[currentMax].last) > 0)
                {
                    currentMax = r;
                    firstMax = true;
                }
            }

            if (currentMax != cell)
            {
                if (firstMax)
                    swapFirstWithLast(currentMax, cell);
                else
                    swapLastWithLast(currentMax, cell);
                heapifyMax(currentMax);
            }
            return swappedroot;
        }

        private void swapLastWithLast(int cell1, int cell2)
        {
            T last = heap[cell2].last;
            updateLast(cell2, heap[cell1].last);
            updateLast(cell1, last);
        }

        private bool heapifyMin(int cell)
        {
            bool swappedroot = false;
            if (2 * cell + 1 < size && comparer.Compare(heap[cell].first, heap[cell].last) > 0) //if given cell has both elements and if the first element is greather than max
            {
                swappedroot = true;
                swapFirstWithLast(cell, cell);
            }

            int currentMin = cell;
            int l = cell * 2 + 1;
            int r = l + 1;

            if (2 * l < size && comparer.Compare(heap[l].first, heap[currentMin].first) < 0) //left child has min element and if it is a smaller than current min 
                currentMin = l;
            if (2 * r < size && comparer.Compare(heap[r].first, heap[currentMin].first) < 0) //right child has min element and if it is a smaller than current min
                currentMin = r;

            if (currentMin != cell) //if we found a smaller element among child nodes
            {
                swapFirstWithFirst(currentMin, cell); //swap min element with parent and one of the child
                heapifyMin(currentMin);
            }
            return swappedroot;
        }

        private void swapFirstWithLast(int cell1, int cell2)
        {
            T first = heap[cell1].first;
            updateFirst(cell1, heap[cell2].last);
            updateLast(cell2, first);
        }

        private void swapFirstWithFirst(int cell1, int cell2)
        {
            T first = heap[cell2].first;
            updateFirst(cell2, heap[cell1].first);
            updateFirst(cell1, first);
        }

    }
}
