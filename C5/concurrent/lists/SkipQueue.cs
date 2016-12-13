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
            public int[] levelTag;

            public Node(int level, T newValue)
            {
                forward = new Node[level];
                levelLock = new object[level];
                lockTaken = new bool[level];
                levelTag = new int[level];
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

        public int Avalible => -1;


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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="action"></param>
        /// <returns></returns>
        private bool lockAll(Node lockOnThis, Func<Boolean> action)
        {
            int s = lockOnThis.levelLock.Length;
            for (int i = 0; i < s; i++)
            {
                Monitor.Enter(lockOnThis.levelLock[i]);
            }
            try
            {
                return action();
            }
            finally
            {
                for (int i = s-1; i >= 0; i--)
                {
                    Monitor.Exit(lockOnThis.levelLock[i]);
                }
            }

            //return lockAll(0, action);
        }

        


        public bool Add(T item)
        {
            //if (addwithShavitLocks(item))
            //    return true;
            //return false;


            //dosent work as of 29/11

            //if (addWithMonitorLocks(item))
            //    return true;
            //return false;

            //WORKS as of 29/11

            if (addWithSimpleLocks(item))
                return true;
            return false;
        }

        private bool addwithShavitLocks(T item)
        {
            Node[] savedNodes = new Node[maxLevel];
            Node node1 = header;
            Node node2;
            Node newNode;
            int localLevel = level;
            bool lockTaken = false;
            bool startOver = false;

            for (int i = node1.forward.Length - 1; i >= 0; i--)
            {

                //if (node1.forward[i] != null) { node2 = node1.forward[i]; }
                //else{ node2 = header; }
                
                while (node1.forward[i] != null && comparer.Compare(node1.forward[i].value, item) <= 0)
                {
                    //node1 = node2;
                    //node2 = node2.forward[i];
                    node1 = node1.forward[i];
                }
                savedNodes[i] = node1;
            }

            //savedNodes = searchAndLock(node1, item, savedNodes);

            //in the paper the first level is locked and a check is performed to see if the value need to be updated
            //we however allow for duplicates so no node ever have to have it's value updated.

            //node1 = getLock(node1, item, 0);
            ////for updating a nodes value, we allow for duplicates so no updating of values
            //node2 = node1.forward[0];
            //if (comparer.Compare(node2.value, item) >= 0)
            //{
            //    node2.value = item;
            //    Monitor.Exit(node1.levelLock[0]);
            //}

            int newLevel = RandomLevel();
            //if (newLevel > level)
            //{
            //    for (int i = level; i < newLevel; i++)
            //    {
            //        //Monitor.Enter(header.levelLock[i]);
            //        savedNodes[i] = header;
            //    }
            //    level = newLevel;
            //}

            

            newNode = new Node(newLevel, item);
            newNode.timeStamp = DateTime.MaxValue;

            bool success = lockAll(newNode, () =>        
            {
               
                   //not <= as our arraylength is level-1, in order to line up level and array indexes
                    for (int i = 0; i < newLevel; i++)
                    {
                        if (savedNodes[i].forward != null)
                        {
                            node1 = getLock(savedNodes[i], item, i, ref lockTaken);
                        }
                        else
                        {
                            node1 = savedNodes[i];
                            Monitor.Enter(node1.levelLock[i], ref lockTaken);
                        }

                        if (i != 0 && newNode.forward[0] != null && (comparer.Compare(newNode.value, newNode.forward[0].value) > 0))
                        {
                            if (lockTaken)
                            {
                                Monitor.Exit(node1.levelLock[i]);
                                lockTaken = false;
                            }
                            return false;
                            //addwithShavitLocks(item);
                        }
                        newNode.forward[i] = node1.forward[i];
                        node1.forward[i] = newNode;
                        if (lockTaken)
                        {
                            Monitor.Exit(node1.levelLock[i]);
                            lockTaken = false;
                        }
                        //savedNodes = doSearch(node1, item, savedNodes);
                        //node1.levelTag[i] = Avalible;
                        //lockTaken = false;
                        //something went wrong, search all over again



                        //savedNodes = doSearch(header, item, savedNodes);
                        //if (comparer.Compare(savedNodes[i].value, newNode.value) != 0)
                        //{
                        //    node1 = getLock(savedNodes[i], item, i, ref lockTaken);


                        //    newNode.forward[i] = node1.forward[i];
                        //    node1.forward[i] = newNode;
                        //    if (lockTaken)
                        //    {
                        //        Monitor.Exit(node1.nodeLock);
                        //        lockTaken = false;
                        //    }
                        //}


                    }
                    return true;
                
            });
            if (!success)
            {
                addwithShavitLocks(item);
            }
            if (newNode.forward[0] != null && (comparer.Compare(newNode.value, newNode.forward[0].value) > 0))
            {
                return false;
               //// startOver = true;
               //addwithShavitLocks(item);
               // savedNodes = doSearch(header, item, savedNodes);
               // for (int i = 0; i < newNode.forward.Length; i++)
               // {
               //     node1 = getLock(savedNodes[i], item, i, ref lockTaken);


               //     newNode.forward[i] = node1.forward[i];
               //     node1.forward[i] = newNode;
               //     if (lockTaken)
               //     {
               //         Monitor.Exit(node1.nodeLock);
               //         lockTaken = false;
               //     }
               // }
            }


            newNode.timeStamp = DateTime.Now;
            Interlocked.Add(ref size, 1);
            return true;
        }

        private bool addWithMonitorLocks(T item)
        {
            Node[] update = new Node[maxLevel];
            Node node1 = header;
            Node node2 = null;
            Node newNode;
            bool lockTaken1 = false;
            bool lockTaken2 = false;

            update = doSearch(node1, item, update);
          
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
                    node1 = update[i];
                    lockTaken1 = false;
                    lockTaken2 = false;
                    if (node1.forward[i] != null)
                    {

                    }
                        //lock the newNode's predecessor
                        Monitor.Enter(node1.levelLock[i], ref lockTaken1);
                    if (node1.forward[i] != null)
                    {
                        Monitor.Enter(node1.forward[i].levelLock[i], ref lockTaken2);
                        Monitor.Enter(node1.levelLock[i], ref lockTaken1);
                        if (comparer.Compare(node1.forward[i].value, item) <= 0)
                        {
                            update = doSearch(node1, item, update);
                            node1 = update[i];
                        }
                        newNode.forward[i] = node1.forward[i];
                        node1.forward[i] = newNode;

                        if (lockTaken1)
                        {
                            Monitor.Exit(node1.levelLock[i]);
                            lockTaken1 = false;
                        }
                        update = doSearch(node1, item, update);
                        node2 = update[i];
                        if (comparer.Compare(node1.forward[i].value, node1.forward[i].value) != 0)
                        {
                            Monitor.Enter(node2.levelLock[i], ref lockTaken2);
                        }
                        
                    }
                    newNode.forward[i] = node1.forward[i];
                    node1.forward[i] = newNode;
                    if (lockTaken1)
                    {
                        Monitor.Exit(node1.levelLock[i]);
                    }
                    else if (lockTaken1 && node2 != null)
                    {
                        Monitor.Exit(node2.levelLock[i]);
                    }
                   
                }
            }
            newNode.timeStamp = DateTime.Now;
            Interlocked.Add(ref size, 1);
            return true;
        }


        private bool addWithSimpleLocks(T item)
        {
            Node[] update = new Node[maxLevel];
            Node node1 = header;
            Node node2;
            Node newNode;
            bool restartSearch = false;
            int iterationLevel;

            //for (int i = level-1; i >= 0; i--)
            //{
            //    //node2 = node1.forward[i];
            //    while (node1.forward[i] != null && comparer.Compare(node1.forward[i].value, item) <= 0)
            //    {
                    
            //        node1 = node1.forward[i];
            //    }
            //    update[i] = node1;
            //}

            //fill update array with search
            update = doSearch(node1, item, update);

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
            
            //return (bool)lockAll(newNode, () =>
            lock (newNode.nodeLock)
            {
                for (int i = 0; i < newLevel; i++)
                {
                    //try
                    //{
                        node1 = update[i];
                        lock (node1.levelLock[i])
                        {
                            if (node1.forward[i] != null)
                            {
                                lock (node1.forward[i].levelLock[i])
                                {
                                    //something changed and we have to re-search
                                    if (comparer.Compare(node1.forward[i].value, item) <= 0)
                                    {
                                        update = doSearch(node1, item, update);
                                        node1 = update[i];
                                    }
                                newNode.forward[i] = node1.forward[i];
                                    node1.forward[i] = newNode;
                                }
                            }
                            else
                            {
                                newNode.forward[i] = node1.forward[i];
                                node1.forward[i] = newNode;
                            }
                            //node1 = update[i];
                            //while (node1.forward[i] != null && comparer.Compare(node1.value, item) <= 0)
                            //{
                            //    node1 = node1.forward[i];
                            //}
                        }
                  
                        //if (comparer.Compare(node1.value, update[i].value) != 0)
                        //{
                        //    lock (update[i].levelLock[i])
                        //    {
                        //        newNode.forward[i] = update[i].forward[i];
                        //        update[i].forward[i] = newNode;
                        //    }
                        //}

                    //}
                    //catch (Exception e)
                    //{
                    //    throw new Exception(i + ", " + update[i].nodeLevel + ", " + update[i].value, e);
                    //}

                    //node1 = getLock(update[i], item, i);

                    //newNode.forward[i] = node1.forward[i];
                    //node1.forward[i] = newNode;

                    //if (update[i].lockTaken[i] && update[i].levelTag[i] == Thread.CurrentThread.ManagedThreadId)
                    //{
                    //    Monitor.Exit(node1.levelLock[i]);
                    //    update[i].lockTaken[i] = false;
                    //    update[i].levelTag[i] = Thread.CurrentThread.ManagedThreadId;
                    //}
                    //try
                    //{
                    //    if (node1.lockTaken[i] && node1.levelTag[i] == Thread.CurrentThread.ManagedThreadId)
                    //    {
                    //        Monitor.Exit(node1.levelLock[i]);
                    //        node1.lockTaken[i] = false;
                    //        node1.levelTag[i] = Avalible;
                    //    }

                    //}
                    //catch (Exception e)
                    //{

                    //    throw e;
                    //}

                }
               
                newNode.timeStamp = DateTime.Now;
                Interlocked.Add(ref size, 1);
                return true;
            }


            //lock (newNode.nodeLock)
            //{
            //}

            //newNode.timeStamp = DateTime.Now;
            // Interlocked.Add(ref size, 1);
            //return true;


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
            //for (int i = 0; i < x.forward.Length; i++)
            //{
            //    while (x.forward[i] != null && comparer.Compare(x.value, x.forward[i].value) <= 0)
            //        x = x.forward[i];

            //    if (x.forward[i] != null)
            //        return false;
            //}

            while (x.forward[0] != null && comparer.Compare(x.value, x.forward[0].value) <= 0)
                x = x.forward[0];

            if (x.forward[0] != null)
                return false;

            return true;
        }

        public T DeleteMax()
        {
            //if (size == 0)
            //    throw new NoSuchItemException();

            //Node[] update = new Node[maxLevel];
            //Node x = header;
            //T retval;
            //for (int i = level - 1; i >= 0; i--)
            //{
            //    while (x.forward[i] != null && x.forward[i].forward[i] != null)
            //        x = x.forward[i];
            //    update[i] = x;
            //}

            //x = x.forward[0];
            //retval = x.value;

            //for (int i = 0; i < x.forward.Length; i++)
            //{
            //    update[i].forward[i] = null;
            //}

            //size--;
            //return retval;

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

        public T DeleteMin()
        {
            if (size == 0)
                throw new NoSuchItemException();

            T retval;
            T value;

            //retval = true;
            var lockTaken = false;
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

        #region helpers

        //private Node[] doSearch(Node head, T item, Node[] update)
        //{
        //    for (int i = head.forward.Length-1; i >= 0; i--)
        //    {
        //        Node node2 = head;
        //        while (node2.forward[i] != null && comparer.Compare(node2.forward[i].value, item) <= 0)
        //        {
        //            head = node2;
        //            node2 = head.forward[i];
        //        }
        //        update[i] = head;
        //    }
        //    return update;
        //}
        private Node[] doSearch(Node head, T item, Node[] update)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                while (head.forward[i] != null && comparer.Compare(head.forward[i].value, item) <= 0)
                {
                    head = head.forward[i];
                }
                update[i] = head;
            }
            return update;
        }



        private Node getLock(Node node1, T value, int level, ref bool lockTaken)
        {
            Node node2;
           
            //lockAquired = false;
            //Node node2 = node1.forward[level]; //orginal psudocode
            //   if (node1.forward[level] != null)
            //   {
            //       node2 = node1.forward[level];
            //   }
            //   else
            //{
            //       node2 = node1;
            //   }

            //   while (comparer.Compare(node2.value, value) <= 0)
            //   {
            //       node1 = node2;
            //       node2 = node1.forward[level];
            //   }

            //orginal code
            //jump to the next node in the update array, and see if we need to go further.  node2 is now update.forward, and node2 is update.forward.forward
            node2 = node1.forward[level];
            while (node2 != null && comparer.Compare(node2.value, value) <= 0)
            {
                node1 = node2;
                node2 = node1.forward[level];
            }

            //my code
            //node2 = node1;
            //while (node2.forward[level] != null && comparer.Compare(node2.forward[level].value, value) <= 0)
            //{
            //    node1 = node2;
            //    node2 = node1.forward[level];
            //}
            try
            {
                Monitor.TryEnter(node1.levelLock[level], ref lockTaken);
            }
            catch (Exception e)
            {

                throw e;
            }
            
            //bool entered = ;


            node2 = node1.forward[level]; //orginal psudocode

            //this next look is to check if something changed before we locked, we only care if the new value is actually
            //greater then our current one, as one of same value would not mess up the structure of the list
            //node2 = node1;
            while (node2 != null && comparer.Compare(node2.value, value) < 0)
            {
                if (lockTaken)
                {
                    Monitor.Exit(node1.levelLock[level]);
                    lockTaken = false;
                }
                
                //node1.levelTag[level] = Avalible;
                 
                node1 = node2;
              
                Monitor.Enter(node1.levelLock[level], ref lockTaken);
                //node1.levelTag[level] = Thread.CurrentThread.ManagedThreadId;
                 
                node2 = node1.forward[level];
            }

            return node1;
        }

        private Node[] searchAndLock(Node head, T item, Node[] update)
        {
            for (int i = head.forward.Length - 1; i >= 0; i--)
            {
                Node node2 = head;
                while (node2.forward[i] != null && comparer.Compare(node2.forward[i].value, item) <= 0)
                {
                    head = node2;
                    node2 = head.forward[i];
                }
                Monitor.Enter(head.levelLock[i]);
                update[i] = head;
            }
            return update;
        }

        

        private int ShavitRandomLevel()
        {
            var lvl = 1;
            while (random.Next(2) == 1)
            {
                lvl++;
            }
            if (lvl > maxLevel)
            {
                return maxLevel;
            }
            return lvl;
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
    #endregion
}
