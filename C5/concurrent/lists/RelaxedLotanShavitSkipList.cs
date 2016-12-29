using System;
using System.Diagnostics;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;

namespace C5.concurrent
{
    public class RelaxedLotanShavitSkipList<T> : IConcurrentPriorityQueue<T>
    {
        /// <summary>
        /// A SkipQueue node
        /// </summary>
        public class Node
        {
            public Node[] forward;
            public int nodeLevel;
            public T value;
            public int deleted;
            public object nodeLock;
            public object[] levelLock;
            public bool tail;
            public int pid;

            public Node(int level, T newValue, bool tail = false)
            {
                forward = new Node[level];
                value = newValue;
                this.tail = tail;
                levelLock = new object[level];
                for (int i = 0; i < level; i++)
                {
                    levelLock[i] = new object();
                }
                nodeLevel = forward.Length;
                deleted = 0;
                nodeLock = new object();
                pid = 0;

            }

            public override string ToString()
            {
                return string.Format("{0}, {1}, {2}", value, tail, pid);
            }

        }

        SCG.IComparer<T> comparer;
        int size, maxLevel;
        int level;
        Node header, tail;

        public RelaxedLotanShavitSkipList() : this(32) { }

        public RelaxedLotanShavitSkipList(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            maxLevel = max;
            size = 0;
            level = 1;
            header = new Node(maxLevel, default(T));

            header.deleted = 2;
            tail = new Node(0, default(T), true);

            tail.deleted = 2;
            for (int i = 0; i < maxLevel; i++)
            {
                header.forward[i] = tail;
            }
        }

        public int Count
        {
            get { return size; }
        }

        public bool Add(T item)
        {
            if (add(item))
                return true;
            return false;
        }

        private bool add(T item)
        {
            Node[] update = new Node[maxLevel];
            Node node1 = header;
            Node node2;
            Node newNode;

            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node2 = node1.forward[i];
                while (!node2.tail && comparer.Compare(node2.value, item) < 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }

            int newLevel = RandomLevel();
            newNode = new Node(newLevel, item);
            lock (newNode.nodeLock)
            {
                for (int i = 0; i < newLevel; i++)
                {
                    node2 = (node1 = update[i]).forward[i];
                    while (!node2.tail && comparer.Compare(node2.value, item) < 0)
                    {
                        node1 = node2;
                        node2 = node2.forward[i];
                    }

                    lock (node1.levelLock[i])
                    {
                        newNode.forward[i] = node1.forward[i];
                        node1.forward[i] = newNode;
                    }

                }
            }
            Interlocked.Increment(ref size);
            return true;
        }


        public SCG.IEnumerable<T> All()
        {
            Node x = header;
            if (x.forward[0].tail)
                throw new NoSuchItemException();


            T[] elements = new T[size];
            int i = 0;
            while (!x.forward[0].tail)
            {
                elements[i] = x.forward[0].value;
                x = x.forward[0];
                i++;
            }
            return elements;
        }

        /// <summary>
        ///  Checks if all the elements in the list are in order, but does not check the pointers on the levels. 
        /// </summary>
        /// <returns>bool</returns>
        public bool Check()
        {
            //no elements, so the list is in order.
            if (size < 2)
                return true;

            //check the queue have the correct number of elements
            Node x = header;
            int j = 0;
            while (x.forward[0].tail != true && j < size)
            {
                x = x.forward[0];
                j++;
            }
            if (j != size)
            {
                throw new Exception("number of elements differ from size: " + "j: " + j + " size: " + size);
                //return false;
            }
            x = header;
            //If the next node is not null and the current node's value is less then or equel to (equel to as we allow for duplicates as of 21/11/2016)
            //we continue to move forward at the bottom level of the list. 
            for (int i = 0; i < x.forward.Length; i++)
            {
                while (x.forward[i].tail != true && comparer.Compare(x.value, x.forward[i].value) <= 0)
                    x = x.forward[i];

                //if the foward pointers of the last element of this level does not point to the tail
                //then something is wrong
                if (!x.forward[i].tail)
                    throw new Exception("next element is not tail: " + x);
                //return false;
            }

            return true;
        }

        public T DeleteMax()
        {
            Node temp;
            T retval;
            Node node1 = header;
            Node node2 = tail;
            Node[] update = new Node[maxLevel];

            if (header.forward[0].tail)
                throw new NoSuchItemException();

            while (true)
            {
                node1 = header;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (!(node2 = node1.forward[i]).tail && node2.deleted != 1 && comparer.Compare(node1.value, node2.value) <= 0)
                    {
                        node1 = node2;
                    }
                }

                if ((Interlocked.Exchange(ref node1.deleted, 1)) == 0)
                {
                    Interlocked.Exchange(ref node1.pid, Thread.CurrentThread.ManagedThreadId);
                    break;
                }
            }

            retval = node1.value;
            temp = node1;
            node1 = header;
            for (int i = maxLevel - 1; i >= 0; i--)
            {

                node2 = node1.forward[i];
                while (!node2.tail && comparer.Compare(node2.value, retval) < 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }
            //assign node 2 back as return node
            node2 = temp;

            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getNodeToLock(update[i], node2, i);
                    lock (node1.levelLock[i])
                    {
                        lock (node2.levelLock[i])
                        {
                            node1.forward[i] = node2.forward[i];
                            node2.forward[i] = node1;
                        }
                    }
                }
            }
            Interlocked.Decrement(ref size);
            return retval;
        }

        public T DeleteMin()
        {
            T retval;
            Node node1;
            Node node2;
            Node[] update = new Node[maxLevel];

            if (header.forward[0].tail)
                throw new NoSuchItemException();

            //search until unmarked (ei. 0) node found, and while the node is not currently being deleted
            node1 = header.forward[0];
            while (!node1.tail)
            {
                if (node1.Equals(header))
                {
                    node1 = node1.forward[0];
                }

                if ((Interlocked.Exchange(ref node1.deleted, 1)) == 0)
                {
                    Interlocked.Exchange(ref node1.pid, Thread.CurrentThread.ManagedThreadId);
                    break;
                }
                node1 = node1.forward[0];
            }

            if (node1.pid == 0)
            {
                throw new Exception();
            }

            //SAVE the value of the node that we just marked for deletion
            retval = node1.value;
            Node retNode = node1;

            node1 = header;
            for (int i = maxLevel - 1; i >= 0; i--)
            {
                node2 = node1.forward[i];
                while (!node2.tail && comparer.Compare(node2.value, retval) < 0)
                {
                    node1 = node2;
                    node2 = node1.forward[i];
                }
                update[i] = node1;
            }
            node2 = node1;
            while (!node2.Equals(retNode))
            {
                node2 = node2.forward[0];
            }

            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length - 1; i >= 0; i--)
                {
                    node1 = getNodeToLock(update[i], node2, i);
                    lock (node1.levelLock[i])
                    {
                        lock (node2.levelLock[i])
                        {
                            node1.forward[i] = node2.forward[i];
                            node2.forward[i] = node1;
                        }
                    }
                }
            }
            Interlocked.Decrement(ref size);
            return retval;
        }

        public T FindMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();

            Node node1 = header;
            Node node2 = null;
            for (int i = maxLevel - 1; i >= 0; i--)
            {
                while (!(node2 = node1.forward[i]).tail && !node2.forward[i].tail)
                    node1 = node2;
            }
            return node1.forward[0].value;

        }

        public T FindMin()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            return header.forward[0].value;
        }

        public bool IsEmpty()
        {
            if (header.forward[0].tail)
                return true;
            return false;
        }

        #region helpers
        /// <summary>
        /// Finds a specific node based on value
        /// </summary>
        /// <param name="node1"></param>
        /// <param name="returnNode"></param>
        /// <param name="level"></param>
        /// <returns></returns>
        private Node getNodeToLock(Node node1, T item, int level)
        {
            Node node2 = node1.forward[level];
            while (!node2.tail && comparer.Compare(node2.value, item) < 0)
            {
                node1 = node2;
                node2 = node2.forward[level];
            }
            return node1;
        }
        /// <summary>
        /// Finds a specific node 
        /// </summary>
        /// <param name="node1"></param>
        /// <param name="returnNode"></param>
        /// <param name="level"></param>
        /// <returns></returns>
        private Node getNodeToLock(Node node1, Node returnNode, int level)
        {
            Node node2 = node1.forward[level];
            while (!node2.tail && !node2.Equals(returnNode))
            {
                node1 = node2;
                node2 = node2.forward[level];
            }
            return node1;
        }

        //private int RandomLevel()
        //{
        //    Random random = new Random(Environment.TickCount + Thread.CurrentThread.ManagedThreadId);
        //    var lvl = 1;
        //    while (random.Next(2) == 1)
        //    {
        //        lvl++;
        //    }
        //    if (lvl > maxLevel)
        //    {
        //        return maxLevel;
        //    }
        //    return lvl;
        //}

        private int RandomLevel()
        {
            Random rng = new Random(Environment.TickCount + Thread.CurrentThread.ManagedThreadId);
            var lvl = 1;
            while (rng.Next(2) == 1 && lvl < maxLevel)
            {
                lvl++;
            }
            return lvl;
        }
    }
    #endregion
}
