using System;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace C5.concurrent
{
    public class SprayList<T> : IConcurrentPriorityQueue<T>
    {

        internal class Node
        {
            internal object nodeLock = new object();
            internal Node[] forward;
            internal T value;
            internal bool marked;
            internal int level = 0;
            internal bool tail = false;
            public Node(int level, T value)
            {
                this.forward = new Node[level + 1];
                this.value = value;
                this.marked = false;
                this.level = level;
            }
            public override string ToString()
            {
                return string.Format("[{0} - {1} - {2} - {3}]", value, marked, tail, level);
            }
        }

        object levelLock = new object();
        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;
        int size, maxLevel, p;
        Node header, tail;

        public SprayList(int p) : this(32, p) { }

        public SprayList(int maxlevel, int p)
        {
            this.comparer = SCG.Comparer<T>.Default;
            this.itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            header = null;
            int max = 1;
            while (max < maxlevel)
                max <<= 1;
            maxLevel = max;
            this.p = p <= 0 ? 1 : p;
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
            int lvl = randomLevel(); //get number of levels a new node has to inserted into 
            int l = 0; //starting level
            bool initialInsert = false;
            Node curr = new Node(0, item);
            while (true)
            {
                if (curr.marked) return true;//if curr logicall deleted, exit
                Node x = header;
                Node[] preds = new Node[maxLevel], succs = new Node[maxLevel];
                if (!initialInsert)
                {
                    //ne wnode have not been inserted yet.
                    for (int i = maxLevel - 1; i >= 0; i--)
                    {
                        while (!x.forward[i].tail && comparer.Compare(x.forward[i].value, item) <= 0) //skip nodes until a position for new node is found.
                            x = x.forward[i];
                        preds[i] = x; //fill with predecessors
                        succs[i] = x.forward[i]; //fill with successors
                    }
                }
                else
                {
                    //new node have been inserted into the list
                    Node next;
                    bool hittail = false;
                    for (int i = maxLevel - 1; i >= 0; i--) //for each level
                    {
                        while (!(next = x.forward[i]).tail && comparer.Compare(next.value, item) < 0) //skip nodes until a nodes with the same value as new node are found.
                            x = next;
                        preds[i] = x; //fill with predecessors
                        succs[i] = x.forward[i];//fill with successors
                    }

                    for (int i = lvl; i >= l; i--) //for each level between lvl and l
                    {
                        x = preds[l - 1]; //assing as predecessor at level l-1
                        if (x.tail) continue;
                        while (!curr.marked && !(next = x.forward[l - 1]).tail && !curr.Equals(next)) //skip nodes until new node is found
                        {
                            x = next;
                            if (x.tail && x.Equals(curr))
                            {
                                hittail = true;
                                break;
                            }
                            if (x.level >= i) //if x node has number of levels at least as i 
                            {
                                preds[i] = x; //assign new predecessor at level i
                                succs[i] = x.forward[i]; //assign new successor at level i
                            }
                        }
                        if (hittail) break;
                    }
                    if (hittail) continue;
                }
                bool breaking = false;
                while (l <= lvl) //continue until all levels are inserted
                {
                    lock (preds[l].nodeLock)
                    {
                        Node pred = preds[l];
                        if (pred.tail || pred.level < 0) break;
                        lock (curr.nodeLock)
                        {
                            if (curr.marked) return true;
                            while (l <= lvl && l <= pred.level) //insert new node into single predecessor until l is not as big as lvl and until l matches predecessor level
                            {
                                if (Validate(pred, succs[l], l, item))
                                {
                                    Node[] newfor = new Node[l + 1]; //create array of size l
                                    for (int i = 0; i < curr.forward.Length; i++) //copy references to new array
                                        newfor[i] = curr.forward[i];
                                    Interlocked.Exchange(ref newfor[l], succs[l]); //add successor at level l to the new array
                                    Interlocked.Exchange(ref curr.forward, newfor); //assign new array to be curr.forward
                                    Interlocked.Exchange(ref pred.forward[l], curr); //assign predecessor at level l to poin to new node
                                    if (l == 0)
                                    {
                                        Interlocked.Increment(ref size);
                                        initialInsert = true;
                                    }
                                    curr.level = l; //number of levels curr node is linked in
                                    if (curr.level == lvl) //all levels have been isnerted
                                        return true;
                                    l++; //next level
                                }
                                else
                                {
                                    breaking = true;
                                    break; //exit while loop
                                }
                            }
                            if (breaking)
                                break; //exit while loopp and restart
                        }
                    }
                }
            }
        }

        public T DeleteMax()
        {
            if (header.forward[0].tail)
                throw new NoSuchItemException();
            int l = int.MaxValue;
            Node curr = null;
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NoSuchItemException();
                Node x = header;
                Node[] preds = new Node[maxLevel], succs = new Node[maxLevel];
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (x.level >= 0 && !x.forward[i].tail && !x.forward[i].forward[i].tail)
                        x = x.forward[i];
                    preds[i] = x;
                    succs[i] = x.forward[i];
                }

                lock (preds[0].nodeLock)
                {
                    if (!preds[0].marked && !preds[0].tail && preds[0].level >= 0 && !(curr = preds[0].forward[0]).tail) //check if predecessor fulfills constrains and get curr node
                    {
                        lock (curr.nodeLock)
                        {
                            if (!curr.tail && curr.forward[0].tail)
                            {
                                l = curr.level; //get node's number of levels
                            }
                            else
                            {
                                curr = null;
                                continue;
                            }
                        }
                    }
                    else continue;
                }

                bool breaking = false;
                while (true) //proceed until all levels are inserted or sychcronization conflict os detected
                {
                    lock (preds[l].nodeLock)
                    {
                        if (!preds[l].tail && preds[l].level >= 0)
                        {
                            lock (curr.nodeLock)
                            {

                                if (!curr.tail && curr.forward[0].tail && curr.level >= 0)
                                {
                                    if (curr.level == l)
                                    {
                                        while (true) //proceed unlti predecessors at level l and l-1 are the same
                                        {
                                            if (ValidatePred(preds[l], curr, l) && curr.Equals(succs[l])) //validate
                                            {
                                                curr.marked = true; //logically delete
                                                Interlocked.Exchange(ref preds[l].forward[l], curr.forward[l]);
                                                curr.level = --l;
                                                if (l == -1) //curr have been removed from all levels
                                                {
                                                    Interlocked.Decrement(ref size);
                                                    return curr.value;
                                                }
                                                if (!preds[l + 1].Equals(preds[l])) //predecessor at level l+1 is not the same at level l
                                                    break; //proceed with locking on another predecessor
                                            }
                                            else
                                            {
                                                breaking = true;
                                                break; //restart serach
                                            }
                                        }
                                    }
                                    else
                                    {
                                        l = curr.level; //get new level
                                        continue; //proceed with new level
                                    }
                                }
                                else
                                {
                                    curr = null;
                                    break; //restart search
                                }
                            }
                        }
                        else
                        {
                            curr = null;
                            break; //restart search
                        }

                    }
                    if (breaking) break; //restart search
                }
            }
        }

        //public T DeleteMax()
        //{
        //    Node curr = null;
        //    int lvl = 0;
        //    while (true)
        //    {
        //        if (header.forward[0].tail)
        //            throw new NoSuchItemException();
        //        Node[] preds = new Node[maxLevel];
        //        Node x = header, next;
        //        bool invalidNode = false;
        //        if (curr == null)
        //        {
        //            for (int i = maxLevel - 1; i >= 0; i--)
        //            {
        //                while (!x.forward[i].tail && !x.forward[i].forward[i].tail)
        //                    x = x.forward[i];
        //                preds[i] = x;
        //            }
        //            lvl = (curr = x.forward[0]).level;
        //        }
        //        else
        //        {
        //            for (int i = maxLevel - 1; i >= 0; i--) //for each level
        //            {
        //                while (!(next = x.forward[i]).tail && comparer.Compare(next.value, curr.value) < 0) //skip nodes until a nodes with the same value as new node are found.
        //                    x = next;
        //                preds[i] = x; //fill with predecessors
        //            }

        //            if ((lvl = curr.level) < 0)
        //            {
        //                curr = null;
        //                continue;
        //            }

        //            for (int i = lvl; i >= 0; i--) //for each level between lvl and l
        //            {
        //                x = preds[0]; //assing as predecessor at level l-1
        //                if (x.tail)
        //                {
        //                    invalidNode = true;
        //                    break;
        //                }
        //                while (!(next = x.forward[0]).tail && !curr.Equals(next)) //skip nodes until new node is found
        //                {
        //                    x = next;
        //                    if (x.tail && x.Equals(curr))
        //                    {
        //                        invalidNode = true;
        //                        break;
        //                    }
        //                    if (x.level >= i) //if x node has number of levels at least as i 
        //                        preds[i] = x; //assign new predecessor at level i
        //                }
        //                if (invalidNode) break;
        //            }
        //            if (invalidNode)
        //            {
        //               // curr = null;
        //                continue;
        //            }
        //        }

        //        if (curr.level < 0)
        //        {
        //            curr = null;
        //            continue;
        //        }
        //        bool breaking = false;
        //        while (lvl >= 0)
        //        {
        //            lock (preds[lvl].nodeLock)
        //            {
        //                if (preds[lvl].level < 0)
        //                    break;
        //                lock (curr.nodeLock)
        //                {
        //                    if (!ValidateNode(curr) || lvl != curr.level)
        //                    {
        //                        curr = null;
        //                        break;
        //                    }

        //                    while (true) //proceed unlti predecessors at level l and l-1 are the same
        //                    {
        //                        if (ValidatePred(preds[lvl], curr, lvl)) //validate
        //                        {
        //                            curr.marked = true; //logically delete
        //                            Interlocked.Exchange(ref preds[lvl].forward[lvl], curr.forward[lvl]);
        //                            curr.level = --lvl;
        //                            if (lvl == -1) //curr have been removed from all levels
        //                            {
        //                                Interlocked.Decrement(ref size);
        //                                return curr.value;
        //                            }
        //                            if (!preds[lvl + 1].Equals(preds[lvl])) //predecessor at level l+1 is not the same at level l
        //                                break; //proceed with locking on another predecessor
        //                        }
        //                        else
        //                        {
        //                            breaking = true;
        //                            break; //restart serach
        //                        }
        //                    }
        //                }
        //            }
        //            if (breaking)
        //                break;
        //        }
        //    }
        //}


        //public T DeleteMax()
        //{
        //    if (header.forward[0].tail)
        //        throw new NoSuchItemException();
        //    int l = int.MaxValue;
        //    Node curr = null;
        //    while (true)
        //    {
        //        if (header.forward[0].tail)
        //            throw new NoSuchItemException();
        //        Node x = header;
        //        Node[] preds = new Node[maxLevel], succs = new Node[maxLevel];
        //        for (int i = maxLevel - 1; i >= 0; i--)
        //        {
        //            while (x.level >= 0 && !x.forward[i].tail && !x.forward[i].forward[i].tail)
        //                x = x.forward[i];
        //            preds[i] = x;
        //            succs[i] = x.forward[i];
        //        }

        //        //lock (preds[0].nodeLock)
        //        //{
        //        //    if (!preds[0].marked && !preds[0].tail && preds[0].level >= 0 && !(curr = preds[0].forward[0]).tail) //check if predecessor fulfills constrains and get curr node
        //        //    {
        //        //        lock (curr.nodeLock)
        //        //        {
        //        //            if (!curr.tail && curr.forward[0].tail)
        //        //            {
        //        //                l = curr.level; //get node's number of levels
        //        //            }
        //        //            else
        //        //            {
        //        //                curr = null;
        //        //                continue;
        //        //            }
        //        //        }
        //        //    }
        //        //    else continue;
        //        //}


        //        l = (curr = preds[0].forward[0]).level;
        //        if (curr.level < 0)
        //        {
        //            curr = null;
        //            continue;
        //        }


        //        bool breaking = false;
        //        while (true) //proceed until all levels are inserted or sychcronization conflict os detected
        //        {
        //            lock (preds[l].nodeLock)
        //            {
        //                if (!preds[l].tail && preds[l].level >= 0)
        //                {
        //                    lock (curr.nodeLock)
        //                    {

        //                        if (!curr.tail && curr.forward[0].tail && curr.level >= 0)
        //                        {
        //                            if (curr.level == l)
        //                            {
        //                                while (true) //proceed unlti predecessors at level l and l-1 are the same
        //                                {
        //                                    if (ValidatePred(preds[l], curr, l) && curr.Equals(succs[l])) //validate
        //                                    {

        //                                        curr.marked = true; //logically delete
        //                                        Interlocked.Exchange(ref preds[l].forward[l], curr.forward[l]);
        //                                        curr.level = --l;
        //                                        if (l == -1) //curr have been removed from all levels
        //                                        {
        //                                            Interlocked.Decrement(ref size);
        //                                            return curr.value;
        //                                        }
        //                                        if (!preds[l + 1].Equals(preds[l])) //predecessor at level l+1 is not the same at level l
        //                                            break; //proceed with locking on another predecessor
        //                                    }
        //                                    else
        //                                    {
        //                                        breaking = true;
        //                                        break; //restart serach
        //                                    }
        //                                }
        //                            }
        //                            else
        //                            {
        //                                l = curr.level; //get new level
        //                                continue; //proceed with new level
        //                            }
        //                        }
        //                        else
        //                        {
        //                            curr = null;
        //                            break; //restart search
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    curr = null;
        //                    break; //restart search
        //                }

        //            }
        //            if (breaking) break; //restart search
        //        }
        //    }
        //}
        //public T DeleteMax()
        //{
        //    int lvl = 0;
        //    Node curr = null;

        //    while (true)
        //    {
        //        if (header.forward[0].tail)
        //            throw new NoSuchItemException();

        //        if (curr == null)
        //        {
        //            curr = GetMaxNode();
        //        }

        //        Node next, x = header;
        //        Node[] preds = new Node[maxLevel];
        //        bool invalidNode = false;
        //        for (int i = maxLevel - 1; i >= 0; i--) //for each level
        //        {
        //            while (!(next = x.forward[i]).tail && comparer.Compare(next.value, curr.value) < 0) //skip nodes until a nodes with the same value as new node are found.
        //                x = next;
        //            preds[i] = x; //fill with predecessors
        //        }

        //        if ((lvl = curr.level) < 0)
        //        {
        //            curr = null;
        //            continue;
        //        }

        //        for (int i = lvl; i >= 0; i--) //for each level between lvl and l
        //        {
        //            x = preds[0]; //assing as predecessor at level l-1
        //            if (x.tail)
        //            {
        //                invalidNode = true;
        //                break;
        //            }
        //            while (!(next = x.forward[0]).tail && !curr.Equals(next)) //skip nodes until new node is found
        //            {
        //                x = next;
        //                if (x.tail || x.Equals(curr) || curr.level < 0)
        //                {
        //                    invalidNode = true;
        //                    break;
        //                }
        //                if (x.level >= i) //if x node has number of levels at least as i 
        //                    preds[i] = x; //assign new predecessor at level i
        //            }
        //            if (invalidNode) break;
        //        }
        //        if (invalidNode || curr.level < 0)
        //        {
        //            curr = null;
        //            continue;
        //        }

        //        bool breaking = false;
        //        while (lvl >= 0)
        //        {
        //            lock (preds[lvl].nodeLock)
        //            {
        //                if (!preds[lvl].tail && preds[lvl].level >= 0)
        //                {
        //                    lock (curr.nodeLock)
        //                    {
        //                        if (!ValidateNode(curr) || lvl != curr.level)
        //                        {
        //                            curr = null;
        //                            break;
        //                        }

        //                        while (true) //proceed unlti predecessors at level l and l-1 are the same
        //                        {
        //                            if (ValidatePred(preds[lvl], curr, lvl)) //validate
        //                            {
        //                                curr.marked = true; //logically delete
        //                                Interlocked.Exchange(ref preds[lvl].forward[lvl], curr.forward[lvl]);
        //                                curr.level = --lvl;
        //                                if (lvl == -1) //curr have been removed from all levels
        //                                {
        //                                    Interlocked.Decrement(ref size);
        //                                    return curr.value;
        //                                }
        //                                if (!preds[lvl + 1].Equals(preds[lvl])) //predecessor at level l+1 is not the same at level l
        //                                    break; //proceed with locking on another predecessor
        //                            }
        //                            else
        //                            {
        //                                breaking = true;
        //                                break; //restart serach
        //                            }
        //                        }
        //                    }
        //                }
        //                else
        //                {
        //                    break;
        //                }
        //            }
        //            if (breaking)
        //                break;
        //        }
        //    }
        //}

        private Node GetMaxNode()
        {
            int minSkips = (int)(1 + (p * Math.Pow(Math.Log(p), 3)));
            int currentSkips = 0;
            Node next, x = header;
            do
            {
                x = header;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (currentSkips <= minSkips && !(next = x.forward[i]).tail && !next.forward[i].tail)
                    {
                        x = next;
                        currentSkips++;
                    }

                    if (currentSkips > minSkips)
                        while (!(next = x.forward[i]).tail && !next.forward[i].tail && !next.forward[i].forward[i].tail && !next.forward[i].forward[i].marked)
                            x = next;
                }
            } while (x.marked || x.level < 0 || x.tail || x.Equals(header));
            return x;
        }

        public T DeleteMin()
        {
            Node curr = null;
            int lvl = 0;
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NoSuchItemException();
                if (curr == null)
                {
                    int d = D(true);
                    int h = H(2, d);
                    int l = L(1);
                    curr = Spray(h, l, d);

                }
                Node[] preds = new Node[maxLevel];
                Node x = header, next;
                bool invalidNode = false;
                for (int i = maxLevel - 1; i >= 0; i--) //for each level
                {
                    while (!(next = x.forward[i]).tail && comparer.Compare(next.value, curr.value) < 0) //skip nodes until a nodes with the same value as new node are found.
                        x = next;
                    preds[i] = x; //fill with predecessors
                }

                if ((lvl = curr.level) < 0)
                {
                    curr = null;
                    continue;
                }

                for (int i = lvl; i >= 0; i--) //for each level between lvl and l
                {
                    x = preds[0]; //assing as predecessor at level l-1
                    if (x.tail)
                    {
                        invalidNode = true;
                        break;
                    }
                    while (!(next = x.forward[0]).tail && !curr.Equals(next)) //skip nodes until new node is found
                    {
                        x = next;
                        if (x.tail && x.Equals(curr))
                        {
                            invalidNode = true;
                            break;
                        }
                        if (x.level >= i) //if x node has number of levels at least as i 
                            preds[i] = x; //assign new predecessor at level i
                    }
                    if (invalidNode) break;
                }
                if (invalidNode)
                {
                    //curr = null;
                    continue;
                }

                bool breaking = false;
                while (true) //proceed until all levels are inserted or sychcronization conflict os detected
                {
                    lock (preds[lvl].nodeLock)
                    {
                        if (!preds[lvl].tail && preds[lvl].level >= 0)
                        {
                            lock (curr.nodeLock)
                            {
                                if (ValidateNode(curr))
                                {
                                    if (curr.level == lvl)
                                    {
                                        while (true) //proceed unlti predecessors at level l and l-1 are the same
                                        {
                                            if (ValidatePred(preds[lvl], curr, lvl)) //validate
                                            {
                                                curr.marked = true; //logically delete
                                                Interlocked.Exchange(ref preds[lvl].forward[lvl], curr.forward[lvl]);
                                                curr.level = --lvl;
                                                if (lvl == -1) //curr have been removed from all levels
                                                {
                                                    Interlocked.Decrement(ref size);
                                                    return curr.value;
                                                }
                                                if (!preds[lvl + 1].Equals(preds[lvl])) //predecessor at level l+1 is not the same at level l
                                                    break; //proceed with locking on another predecessor
                                            }
                                            else
                                            {
                                                breaking = true;
                                                break; //restart serach
                                            }
                                        }
                                    }
                                    else
                                    {
                                        lvl = curr.level; //get new level
                                        continue; //proceed with new level
                                    }
                                }
                                else
                                {
                                    curr = null;
                                    break; //restart search
                                }
                            }
                        }
                        else
                        {
                            //curr = null;
                            break; //restart search
                        }

                    }
                    if (breaking) break; //restart search
                }

                //bool breaking = false;
                //while (lvl >= 0)
                //{
                //    lock (preds[lvl].nodeLock)
                //    {

                //        lock (curr.nodeLock)
                //        {
                //            if (!ValidateNode(curr) && lvl != curr.level)
                //            {
                //                curr = null;
                //                break;
                //            }
                //            while (true) //proceed unlti predecessors at level l and l-1 are the same
                //            {
                //                if (ValidatePred(preds[lvl], curr, lvl)) //validate
                //                {
                //                    curr.marked = true; //logically delete
                //                    Interlocked.Exchange(ref preds[lvl].forward[lvl], curr.forward[lvl]);
                //                    curr.level = --lvl;
                //                    if (lvl == -1) //curr have been removed from all levels
                //                    {
                //                        Interlocked.Decrement(ref size);
                //                        return curr.value;
                //                    }
                //                    if (!preds[lvl + 1].Equals(preds[lvl])) //predecessor at level l+1 is not the same at level l
                //                        break; //proceed with locking on another predecessor
                //                }
                //                else
                //                {
                //                    breaking = true;
                //                    break; //restart serach
                //                }
                //            }
                //        }
                //    }
                //    if (breaking)
                //        break;
                //}
            }
        }

        public T FindMax()
        {
            while (true)
            {
                if (header.forward[0].tail)
                    throw new NoSuchItemException();
                Node x = header;
                Node curr = null;
                for (int i = maxLevel - 1; i >= 0; i--)
                {
                    while (!x.forward[i].tail && !x.forward[i].marked && !x.forward[i].forward[i].tail)
                        curr = x = x.forward[i];
                }
                if (curr == null) continue;
                if (curr.forward[0].tail)
                {
                    if (curr.tail)
                        continue;
                    else
                        return curr.value;
                }
                else
                    return curr.forward[0].value;
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
            Random rng = new Random(Environment.TickCount + Thread.CurrentThread.ManagedThreadId);
            int level = 0;
            int p = 1;
            while (rng.Next(2) < p && level < maxLevel - 1)
                level = level + 1;
            return level;
        }

        private bool Validate(Node pred, Node succ, int level, T item)
        {
            if (succ.tail && !pred.marked && pred.level >= 0 && succ.level >= 0 && pred.forward[level].Equals(succ) && comparer.Compare(pred.value, item) <= 0)
                return true;
            if (!succ.tail && !pred.marked && pred.level >= 0 && succ.level >= 0 && !succ.marked && pred.forward[level].Equals(succ) &&
                comparer.Compare(pred.value, item) <= 0 && comparer.Compare(succ.value, item) >= 0)
                return true;
            return false;
        }

        private bool ValidatePred(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && pred.level >= 0 && succ.level >= 0 && pred.forward[level].Equals(succ))
                return true;
            return false;
        }

        private bool ValidateUnmarked(Node pred, Node succ, int level)
        {
            if (!succ.tail && !pred.marked && pred.level >= 0 && succ.level >= 0 && !succ.marked && pred.forward[level].Equals(succ))
                return true;
            return false;
        }
        //private bool ValidateMarked(Node pred, Node succ, int level)
        //{
        //    if (!succ.tail && !pred.marked && pred.level >= 0 && succ.marked && succ.level >= 0 && succ.pid == Thread.CurrentThread.ManagedThreadId && pred.forward[level].Equals(succ))
        //        return true;
        //    return false;
        //}

        private bool ValidateNode(Node n)
        {
            if (!n.tail && n.level >= 0)
                return true;
            return false;
        }

        private int H(int K, int D)
        {
            if (K < 2)
                K = 2;
            int H = (int)Math.Log(p) + K;
            while (H % D != 0)
                H++;
            return H;
        }

        private int D(bool defvalue)
        {
            if (defvalue)
                return 1;
            else
                return (int)Math.Max(1, Math.Floor(Math.Log(Math.Log(p))));
        }

        private int L(int M)
        {
            if (M < 1)
                M = 1;
            int L = (int)(M * Math.Pow(Math.Log(p), 3));
            return L == 0 ? 1 : L;
        }


        private Node Spray(int H, int L, int D)
        {
            Random rng = new Random(Environment.TickCount + Thread.CurrentThread.ManagedThreadId);
            Node x = header;
            do
            {
                x = header;
                int l = H;
                while (l >= 0)
                {
                    int j = rng.Next(0, L + 1);
                    while (j > 0 && !x.forward[l].tail)
                    {
                        x = x.forward[l];
                        j--;
                    }
                    l = l - D;
                }
            } while (x.marked || x.level < 0 || x.tail || x.Equals(header));
            return x;
        }
    }
}
