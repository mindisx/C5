using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class HellerSkipListv1<T> : IConcurrentPriorityQueue<T>
    {

        internal class Node
        {
            internal object nodeLock = new object();
            internal Node[] forward;
            internal T value;
            internal bool marked;
            internal int pid = 0;
            internal bool tail = false;
            public Node(int level, T value)
            {
                this.forward = new Node[level + 1];
                this.value = value;
                marked = false;
            }
            public override string ToString()
            {
                return string.Format("[{0} - {1}]", value, marked);
            }
        }

        object levelLock = new object();
        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel;
        Node header, tail;

        public HellerSkipListv1() : this(32) { }

        public HellerSkipListv1(int maxlevel)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            header = null;
            int max = 1;
            while (max < maxlevel)
                max <<= 1;
            maxLevel = max;
            tail = new Node(maxLevel - 1, default(T));
            tail.tail = true;
            header = new Node(maxLevel - 1, default(T));
            for (int i = 0; i < maxLevel; i++)
            {
                header.forward[i] = tail;
                tail.forward[i] = tail;
            }
        }


        public int Count
        {
            get
            {
                return size;
            }
        }

        public SCG.IEnumerable<T> All()
        {
            throw new NotImplementedException();
        }

        public bool Check()
        {
            if (size == 0)
                return true;

            for (int i = 0; i <= maxLevel - 1; i++)
            {
                Node x = header;
                //header is 0 (and we allow for dupliates as of 21/11/2016), so we have to check if the previous node is greater then or equel to the current one
                while (!x.forward[i].tail && comparer.Compare(x.value, x.forward[i].value) <= 0)
                    x = x.forward[i];

                //If the the next element isen't null, there's an element next x (the supposed last and least element), and the list is out of order
                if (!x.forward[i].tail)
                    return false;

            }
            return true;
        }

        public bool Add(T item)
        {
            int lvl = randomLevel(); //number of levels a new node has to be inserted
            bool initialInsert = false; 
            Node curr = new Node(0, item); //initialize new node with forward.lenght = 1
            for (int l = 0; l <= lvl; l++) //iterate through lvl 
            {
                bool levelInsert = false;
                while (true)
                {
                    if (curr.marked) //if new node has been logically deleted
                        return true;
                    Node x = header;
                    Node[] preds = new Node[maxLevel], succs = new Node[maxLevel]; //array of predecessors and successors
                    if (!initialInsert) // new node have not been iserted into the list 
                    {
                        for (int i = maxLevel - 1; i >= 0; i--)
                        {
                            while (!x.forward[i].tail && comparer.Compare(x.forward[i].value, item) <= 0)
                                x = x.forward[i];
                            preds[i] = x;
                            succs[i] = x.forward[i];
                        }
                    }
                    else //new node have been iserted into the list 
                    {
                        Node next;
                        bool hittail = false;
                        for (int i = maxLevel - 1; i >= 0; i--) //fill preds array with node where value is smaller than item
                        {
                            while (!(next = x.forward[i]).tail && comparer.Compare(next.value, item) < 0)
                                x = next;
                            preds[i] = x;
                            succs[i] = x.forward[i];
                        }

                        x = preds[l - 1]; //assign predecessor at level l-1 to be x 
                        if (x.tail) continue;
                        while (!curr.marked && !(next = x.forward[l - 1]).tail && !curr.Equals(next)) //search for curr node at level l-1 
                        {
                            x = next;
                            if (x.tail || curr.Equals(x))
                            {
                                hittail = true;
                                break;
                            }
                            if (x.forward.Length - 1 >= l) //if true, then x becomes new predecessor at level l 
                            {
                                preds[l] = x;
                                succs[l] = x.forward[l];
                            }
                        }
                        if (hittail) continue; //restart
                    }
                    lock (preds[l].nodeLock)
                    {
                        if (preds[l].tail)
                            continue;
                        lock (curr.nodeLock)
                        {
                            if (curr.marked) return true;
                            if (Validate(preds[l], succs[l], l, item)) //if synchronization conflict have not occured at level l
                            {
                                Node[] newfor = new Node[l + 1]; //create array of size l
                                for (int i = 0; i < curr.forward.Length; i++) //copy references to new array
                                    newfor[i] = curr.forward[i];
                                Interlocked.Exchange(ref newfor[l], succs[l]); //add successor at level l to the new array
                                Interlocked.Exchange(ref curr.forward, newfor);//assign new array to be curr.forward
                                Interlocked.Exchange(ref preds[l].forward[l], curr); //assign predecessor at level l to poin to new node
                                levelInsert = true; //node have been inserted into l level
                                if (l == 0) //if node have been inserted into the first level
                                {
                                    Interlocked.Increment(ref size); //increase size of the list
                                    initialInsert = true; //initial insert have happened
                                }
                            }
                        }
                    }
                    if (levelInsert) break; //continue with next level
                }
            }
            return true;
        }
        public T DeleteMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            T retval = default(T);
            bool initialdelete = false;
            int l = int.MaxValue, currlvl = int.MaxValue;
            Node curr = null;
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NoSuchItemException();
                Node x = header;
                Node[] preds = new Node[maxLevel];
                if (!initialdelete)
                {
                    for (int i = maxLevel - 1; i >= 0; i--)
                    {
                        while (!x.forward[i].tail && !x.forward[i].forward[i].tail)
                            x = x.forward[i];
                        preds[i] = x;
                    }
                }
                else
                {
                    Node next;
                    bool hittail = false;
                    for (int i = maxLevel - 1; i >= 0; i--)
                    {
                        while (!(next = x.forward[i]).tail && comparer.Compare(next.value, retval) < 0)
                            x = next;
                        preds[i] = x;
                    }

                    x = preds[l];//assign predecessor at level l to be x 
                    if (x.tail) continue;
                    while (!(next = x.forward[l]).tail && !next.Equals(curr))//search for curr node at level l
                    {
                        x = next;
                        if (x.tail || curr.Equals(x))
                        {
                            hittail = true;
                            break;
                        }
                        if (x.forward.Length - 1 >= l)//if true, then x becomes new predecessor at level l 
                            preds[l] = x;
                    }
                    if (hittail) continue; //restart
                }

                if (!initialdelete) //a node have not been logically deleted
                {
                    lock (preds[0].nodeLock)
                    {
                        if (preds[0].tail) continue;
                        curr = preds[0].forward[0]; //find a node to delete
                        lock (curr.nodeLock)
                        {
                            if (ValidateUnmarked(preds[0], curr, 0)) //if validation pass
                            {
                                curr.marked = true; //logically delete node
                                curr.pid = Thread.CurrentThread.ManagedThreadId; //assign thread id
                                initialdelete = true; //logicall deleteion happened
                                l = curr.forward.Length - 1; //obtain level from which curr has to be deleted
                                retval = curr.value; //get value
                            }
                            else
                                continue; //restart
                        }
                    }
                }

                lock (preds[l].nodeLock)
                {
                    if (preds[l].tail) continue;
                    lock (curr.nodeLock)
                    {
                        if (curr.tail) continue;
                        if (ValidateMarked(preds[l], curr, l)) //if validation pass
                        {
                            Interlocked.Exchange(ref preds[l].forward[l], curr.forward[l]); //remove curr node from level
                            currlvl = --l; //next level
                        }
                        else
                            continue;
                    }
                }
                if (currlvl == -1) //when true, all levels have been deleted
                    break;
            }
            Interlocked.Decrement(ref size);
            return retval; 
        }

        public T DeleteMin()
        {
            T retavl = default(T);
            int currlvl = int.MaxValue;
            while (true)
            {
                Node pred = header;
                lock (pred.nodeLock)
                {
                    Node curr = header.forward[0];
                    lock (curr.nodeLock)
                    {
                        if (curr.tail)
                            throw new NoSuchItemException();
                        if (ValidateUnmarked(pred, curr, 0)) //if validation holds
                        {
                            curr.marked = true; //logically delete
                            curr.pid = Thread.CurrentThread.ManagedThreadId; //assign current thread id
                            retavl = curr.value;
                            for (int i = curr.forward.Length - 1; i >= 0; i--) //remove curr from all levels
                            {
                                Interlocked.Exchange(ref pred.forward[i], curr.forward[i]);
                                currlvl = i;
                            }
                        }
                    }
                }
                if (currlvl == 0) //if true, all levels have been removed
                    break;
            }
            Interlocked.Decrement(ref size);
            return retavl;
        }

        public T FindMax()
        {
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NotImplementedException();
                Node x = header;
                for (int i = maxLevel-1; i >= 0; i--)
                {
                    while (!x.forward[i].tail && !x.forward[i].forward[i].tail)
                        x = x.forward[i];
                }
                if (!x.tail || !x.forward[0].tail)
                    return x.forward[0].value;
            }
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

        private int randomLevel()
        {
            Random rng = new Random();
            int level = 0;
            int p = 1;
            while (rng.Next(2) < p && level < maxLevel - 1)
                level = level + 1;
            return level;
        }

        private bool Validate(Node pred, Node succ, int level, T item)
        {
            if (succ.tail && !pred.marked && pred.forward[level].Equals(succ) && comparer.Compare(pred.value, item) <= 0)
                return true;
            if (!succ.tail && !pred.marked && !succ.marked && pred.forward[level].Equals(succ) &&
                comparer.Compare(pred.value, item) <= 0 && comparer.Compare(succ.value, item) >= 0)
                return true;
            return false;
        }

        private bool ValidateUnmarked(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && !succ.marked && pred.forward[level].Equals(succ))
                return true;
            return false;
        }
        private bool ValidateMarked(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && succ.marked && succ.pid == Thread.CurrentThread.ManagedThreadId && pred.forward[level].Equals(succ))
                return true;
            return false;
        }
    }
}
