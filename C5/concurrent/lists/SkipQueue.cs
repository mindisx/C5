using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class SkipQueue<T> : IConcurrentPriorityQueue<T>
    {
        /// <summary>
        /// 
        /// </summary>
        public class Node
        {
            //An array the size of the "height" of the node, holds pointers to nodes on the seperate levels. 
            public Node[] forward;
            public int nodeLevel;
            public T value;
            public DateTime timeStamp;
            public int deleted;
            public object nodeLock;
            public object[] levelLock;
            public bool[] lockTaken;

            public Node(int level, T newValue)
            {
                forward = new Node[level];
                levelLock = new object[level];
                lockTaken = new bool[level];
                for (int i = 0; i < level; i++)
                {
                    levelLock[i] = new object();
                }
                for (int i = 0; i < level; i++)
                {
                    lockTaken[i] = false;
                }
                //might need to delete this, pugh says we do not keep track of the level of the node, in the node itself
                nodeLevel = forward.Length;
                value = newValue;
                timeStamp = DateTime.Now;
                deleted = 0;
                nodeLock = new object();
            }

            public override string ToString()
            {
                return string.Format("[{0}]", value);
            }
        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel;
        int level;
        Node header;
        Random random;
        volatile bool lockAquired = false;


        public SkipQueue() : this(32)
        {
        }

        public SkipQueue(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            maxLevel = max;
            size = 0;
            level = 1;
            header = new Node(maxLevel, default(T));
            random = new Random();
            lockAquired = false;

        }

        public int Count
        {
            get { return size; }
        }

        public bool Add(T item)
        {
            Node[] update = new Node[maxLevel];
            Node node1 = header;
            Node node2;
            Node newNode;
            for (int i = level-1; i >= 0; i--)
            {
                node2 = node1.forward[i];
                while (node2 != null && comparer.Compare(node2.value, item) <= 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                update[i] = node1;
            }

            //here, in the psudocode, a lock on the bottom level is obtained
            //and a check is performed to see that the insertion key is not the same as the current one
            //IF it is, the value of he key is updated

            int newLevel = RandomLevel();
            if (newLevel > level)
            {
                for (int i = level; i < newLevel; i++)
                {
                    update[i] = header;
                }
                level = newLevel;
            }
            newNode = new Node(newLevel, item);
            newNode.timeStamp = DateTime.MaxValue;

            lock (newNode.nodeLock)
            {
                
                for (int i = 0; i < newLevel; i++)
                {
                    try
                    {

                        //node1 = update[i];
                        //lock (update[i].levelLock[i])
                        //{
                        //    newNode.forward[i] = update[i].forward[i];
                        //    update[i].forward[i] = newNode;
                        //}
                        node1 = getLock(update[i], item, i);

                        newNode.forward[i] = node1.forward[i];
                        node1.forward[i] = newNode;

                        if (update[i].lockTaken[i])
                        {
                            Monitor.Exit(node1.levelLock[i]);
                            update[i].lockTaken[i] = false;
                        }
                    }
                    catch (Exception e)
                    {

                        throw e;
                    }
                }
               

            }

            newNode.timeStamp = DateTime.Now;
            Interlocked.Add(ref size, 1);
            return true;

        }

        public SCG.IEnumerable<T> All()
        {
            if (size == 0)
                throw new NoSuchItemException();

            Node x = header;
            T[] elements = new T[size];
            int i = 0;
            while (x.forward[0] != null)
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
            if (size == 0)
                return true;

            Node x = header;
            //If the next node is not null and the current node's value is less then or equel to (equel to as we allow for duplicates as of 21/11/2016)
            //we continue to move forward at the bottom level of the list. 
            for (int i = 0; i < x.forward.Length; i++)
            {
                while (x.forward[i] != null && comparer.Compare(x.value, x.forward[i].value) <= 0)
                    x = x.forward[i];

                if (x.forward[i] != null)
                    return false;

            }


            //If the the next element isen't null, there's an element next x (the supposed last and least element), and the list is out of order


            return true;
        }

        public T DeleteMax()
        {
            if (size == 0)
                throw new NoSuchItemException();

            Node[] update = new Node[maxLevel];
            Node x = header;
            T retval;
            for (int i = level-1; i >= 0; i--)
            {
                while (x.forward[i] != null && x.forward[i].forward[i] != null)
                    x = x.forward[i];
                update[i] = x;
            }

            x = x.forward[0];
            retval = x.value;

            for (int i = 0; i < x.forward.Length; i++)
            {
                update[i].forward[i] = null;
            }

            size--;
            return retval;
        }

        public T DeleteMin()
        {
            if (size == 0)
                throw new NoSuchItemException();

            T retval;
            T value;

            //retval = true;

            DateTime time = DateTime.Now;
            bool marked;
            Node node1 = header.forward[0];
            Node node2;
            Node[] savedNodes = new Node[maxLevel];
            while (node1.forward[0] != null && node1.timeStamp < time)
            {
                Interlocked.Exchange(ref node1.deleted, 1);
                //unmarked node was found
                if (node1.deleted != 1)
                    break;

                node1 = node1.forward[0];
            }

            //if (node1.forward[0] != null)
            //{
            //    //aka key
            //    value = node1.value;
            //}

            //Shavit has a key associted with each node, but we do not, and just rely on the value
            //so we will use that to compare with

            value = node1.value;
            node1 = header;
            for (int i = level; i > 0; i--)
            {
                node2 = node1.forward[i];
                while (node2.forward[i] != null && comparer.Compare(node2.value, value) > 0)
                {
                    node1 = node2;
                    node2 = node2.forward[i];
                }
                savedNodes[i] = node1;
            }

            node2 = node1;
            while (comparer.Compare(node2.value, value) != 0)
            {
                node2 = node2.forward[0];
            }
            retval = node2.value;
            lock (node2.nodeLock)
            {
                for (int i = node2.forward.Length; i > 0; i--)
                {
                    lock (node1.forward[i].nodeLock)
                    {
                        lock (node2.forward[i].nodeLock)
                        {
                            node1.forward[i] = node2.forward[i];
                            node2.forward[i] = node1;
                        }
                    }
                }
            }
            return retval;

        }

        public T FindMax()
        {
            if (size == 0)
                throw new NoSuchItemException();

            Node x = header;
            for (int i = level; i >= 0; i--)
            {
                while (x.forward[i] != null && x.forward[i].forward[i] != null)
                    x = x.forward[i];
            }
            return x.forward[0].value;
        }

        public T FindMin()
        {
            if (size == 0)
                throw new NoSuchItemException();
            return header.forward[0].value;
        }

        public bool IsEmpty()
        {
            if (size == 0)
                return true;
            return false;
        }

        //helpers


        private Node getLock(Node node1, T value, int level)
        {
            //lockAquired = false;
            //Node node2 = node1.forward[level]; //orginal psudocode
            Node node2 = node1;
            while (node2.forward[level] != null && comparer.Compare(node2.value, value) <= 0)
            {
                node1 = node2;
                node2 = node1.forward[level];
            }

            Monitor.Enter(node1.levelLock[level], ref node1.lockTaken[level]);

            //node2 = node1.forward[level]; //orginal psudocode
            node2 = node1;
            while (node2.forward[level] != null && comparer.Compare(node2.value, value) <= 0)
            {
                try
                {
                    if (node1.lockTaken[level])
                    {
                        Monitor.Exit(node1.levelLock[level]);
                        node1.lockTaken[level] = false;
                    }
                }
                catch (Exception e)
                {

                    throw e;
                }

                node1 = node2;
                try
                {
                    Monitor.Enter(node1.levelLock[level], ref node2.lockTaken[level]);
                }
                catch (Exception e )
                {
                    
                    throw e;
                } 
                
                node2 = node1.forward[level];
            }

            return node1;
        }

        private int RandomLevel()
        {
            var lvl = 1;
            while (random.Next(2) == 1 && lvl < maxLevel)
            {
                lvl++;
            }
            return lvl;
        }
    }
}
