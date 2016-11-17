using System;
using System.Collections;
using SCG = System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace C5.concurrent.skiplist
{
    public class PughSkiplist<T> : IConcurrentPriorityQueue<T>
    {
        public class Node
        {
            //forward in paper
            //An array the size of the "height" of the node, holds pointers to nodes on the seperate levels. 
            public Node[] next;
            public T value;

            public Node(int level, T newValue)
            {
                next = new Node[level];
                value = newValue;
            }

            public override string ToString()
            {
                return string.Format("[{0}]", value);
            }



            //helper to print the skiplist
            //inspired by http://stackoverflow.com/a/1649223/2404789
            //public void PrintList(string indent, bool last, int size, Node headNode)
            //{

            //    StringBuilder SB = new StringBuilder();
            //    SB.Append(indent);
            //    if (last)
            //    {
            //        SB.Append("\\-");
            //        indent += "  ";
            //    }
            //    else
            //    {
            //        SB.Append("|-");
            //        indent += "| ";
            //    }
            //    SB.Append();

            //    //for (int i = 0; i < size; i++)
            //    PrintList(indent, next[size] == null, size - 1);
            //        //next[size].PrintList(indent, next[size] == null, size-1);
            //}
        }

        SCG.IComparer<T> comparer;
        SCG.IEqualityComparer<T> itemEquelityComparer;

        #region Properties

        int levels, maxLevel, size;
        public Node head;

        //with maxlevel 16 2^16 elements, later Pugh revisions (in the cookbook to 2^32 elements)
        private Random random;

        #endregion

        public PughSkiplist() : this(32)
        {
        }


        /// <summary>
        /// Init, constructor
        /// </summary>
        public PughSkiplist(int max)
        {
            comparer = SCG.Comparer<T>.Default;
            itemEquelityComparer = SCG.EqualityComparer<T>.Default;
            maxLevel = max;
            //num of elements, not counting sentinel nodes head and tail
            size = 0;
            //initial the levels of the list to 1. This might seem a little counter intuative, but bear with me: 
            //The overall levels of the list is 1, so we have 1 level. However since the index of an array start at 0, the bottom of the []next array will be index 0
            levels = 1;
            head = new Node(maxLevel, default(T));
            //tail is the last element on all levels
            //instead of using the tail, or NIL employed by Pugh, we simple use NULL to denote the end of a list. 
            //pugh: if p=1/2 using maxlevel = 16 is apporiate for data structures containing 2^16 elemnts. Pugh (in the cookbook for skiplists) later revises
            //this number to 2^32 or 4.3 billion elements. If using using the "fixing the dice" approach describe in the orginal paper, the levels will never reach
            //more then one above the current maximum. 
            random = new Random();

        }

        /// <summary>
        ///When generating a new random level, there is a chance that the new level will be several levels above the current one
        ///this could me a huge gap in levels. The fix is to only only use one plux the current maximum level as the new level of a node
        ///Pugh's calls this "fixing the dice" but tells purits to avoid it as the level of a node is no longer compleatly random.
        /// </summary>
        /// <returns></returns>
        private int RandomLevel()
        {
            var lvl = 1;
            //gets a random non negative integer less then the upper bound (2) aka 0 or 1
            //which means a 50% probablity that it will be in level 1, 25% in level 2, 12.5 in level 3 and so on
            //and while it is larger then p=1/2 aka while it it equel to 1.
            //Here we are "fixing the dice" my making the new level less then then or equel to the total amount of levels
            //&& lvl <= Levels+1
            //use above for the "fixin the dice"
            while (random.Next(2) == 1 && lvl < maxLevel)
            {
                lvl++;
            }
            return lvl;
        }
        
        public bool Add(T item)
        {
            //Update contains a pointer to the rightmost node of level i or higher the is to the left of the location fof the insert/delete
            //in our case we can take the node right before the tail on each level.
            Node[] update = new Node[maxLevel];
            Node x = head;

            //first do the search
            //use levels-1 in order to get the right index, as the height of our nodes are arrays with indicies
            //starting at 0. Levels -1 with >=0 will iterate from 9 to 0 if we have 10 levels.
            for (int i = levels; i >= 0; i--)
            {
                while (x.next[i] != null && comparer.Compare(x.next[i].value, item) < 0)
                {
                    if (x.next[0] == null)
                        //this is the last element
                        break;
                    x = x.next[i];
                }
                update[i] = x;
            }

            int newLevel = RandomLevel();
            if (newLevel > levels)
            {
                for (int i = levels; i < newLevel; i++)
                {
                    update[i] = head;
                }
                levels = newLevel;
            }
            x = new Node(newLevel, item);
            for (int i = 0; i < newLevel; i++)
            {
                x.next[i] = update[i].next[i];
                update[i].next[i] = x;
            }
            size++;
            return true;
        }


        public T FindMin()
        {
            if (size == 0)
            {
                throw new NoSuchItemException();
            }
            Node x = head;
            Node[] update = new Node[maxLevel];
            T retval = x.next[0].value;

            for (int i = levels; i >= 0; i--)
            {
                while (x != head && x.next[i] != null && comparer.Compare(x.next[i].value, x.value) < 0)
                {
                    x = x.next[i];
                }
            }
            
            return x.next[0].value;
        }

        private T Find(T searchValue)
        {
            T retval = head.next[0].value;
            Node x = head;
            Node[] update = new Node[maxLevel];
            for (int i = levels; i >= 0; i--)
            {
                while (x.next[i] != null && comparer.Compare(x.next[i].value, searchValue) < 0)
                {
                    x = x.next[i];
                }
                update[i] = x;
            }
            x = x.next[0];

            if (x != head && comparer.Compare(searchValue, x.value) == 0)
            {
                retval = x.value;
            }

            return retval;
        }

        public T FindMax()
        {
            if(size == 0)
            {
                throw new NoSuchItemException();
            }
            Node[] update = new Node[maxLevel];
            Node x = head;
            T retval;

            //with max, we do need to search. 
            for (int i = levels; i >= 0; i--)
            {
                //currentMax = x.next[i].value;
                while (x.next[i] != null && comparer.Compare(x.next[i].value, x.value) >= 0)
                {
                    x = x.next[i];
                }
                //update[i] = x;
            }
            return retval = x.value;
        }

        public T DeleteMin()
        {
            if (size == 0)
            {
                throw new NoSuchItemException();
            }

            Node[] update = new Node[maxLevel];
            Node x = head;
            T retval;
            //No need to search, Just take the first value as the list is already ordred. 
            x = x.next[0];
            retval = x.value;
            //fill the update array with the header
            for (int i = 0; i < levels; i++)
            {
                update[i] = head;
            }

            //We get the levels of the node to replace the one we deleted, in order to fix the pointers from the head to. 
            var lvl = GetLength(x.next);
            //fill the "gap" left by the removal of the item, by saying the the next item from the head
            //is now the "deletedItem.next" node
            for (int i = 0; i < lvl; i++)
            {
                while (x.next[i] != null && comparer.Compare(x.next[i].value, x.value) > 0)
                {
                    x = x.next[i];    
                }

                update[i].next[i] = x.next[i];

            }

            // Do we need to shrink the levels? 
            while (levels > 1 && head.next[levels] == null)
            {
                levels--;
            }
            size--;
            return retval;

        }

        public T DeleteMax()
        {
            if (size == 0)
            {
                throw new NoSuchItemException();
            }

            Node[] update = new Node[maxLevel];
            Node[] prevNodes = new Node[maxLevel];
            Node x = head;
            Node temp = null;
            //T curr;
            int height;
            T retval;
            //with max, we do need to search. using levels-1 and >= 0 to include zero and not iterate one too many times.
            for (int i = levels-1; i >= 0; i--)                                     
            {
                while (x.next[i] != null && comparer.Compare(x.next[i].value, x.value) >= 0)
                {

                    temp = x;
                    x = x.next[i];
                }
                //the node before x
                update[i] = x;
            }
            //assign return value, which is x. The node to the right of temp
            retval = x.value;
            int length = GetLength(update);
            //we removed the last element, so we update the references
            for (int i = 0; i < length; i++)
            {
                //no move forward and check if the next value is less then the return value, if it is update the array
                //so we get to the end, and then assign the very end to null
                while (update[i].next[i] != null && comparer.Compare(update[i].next[i].value, retval) < 0)
                {
                    update[i] = update[i].next[i];
                }
                update[i].next[i] = null;
            }

            // Do we need to shrink the levels? 
            while (levels > 1 && head.next[levels] == null)
            {
                levels--;
            }
            size--;
            return retval;
        }
        //array helper
        private static int GetLength(Node[] arr)
        {
            int count = 0;
            foreach (var element in arr)
            {
                if (element != null)
                {
                    count++;
                }   
            }
            return count;
        }

        public bool Check()
        {
            throw new NotImplementedException();
        }

        public bool IsEmpty()
        {
            if (size == 0) { return true; }
            return false;
        }

        /// <summary>
        ///  
        /// </summary>
        /// <returns></returns>
        public SCG.IEnumerable<T> All()
        {
            if (size == 0)
                throw new NoSuchItemException();

            Node x = head;
            T[] elements = new T[size]; 
            int i = 0;
            while (x.next[0] != null)
            {
                elements[i] = x.next[0].value;
                x = x.next[0];
                i++;
            }
            return elements;
            //T[,] elements = new T[size, levels];
            //for (int i = 0; i < levels; i++)
            //{
            //    while (head.next[i] != null)
            //    {
            //        elements[]
            //    }
            //}
            //if (flatten)
            //{
            //    return Flatten(elements);
            //}
            //else
            //{
            //    return elements;
            //}

        }

        //helper
        private T[] Flatten(T[,] multiArray)
        {
            return multiArray.Cast<T>().ToArray();
        }

        public int Count
        {
            get { return size; }
        }


    }
}
