package org.kucro3.collection;

import java.lang.ref.SoftReference;

public class ResilientCompoundList<T> {
    public ResilientCompoundList()
    {

    }
    public T get(int index)
    {
        Location location = locate(index);
        return location.located.get(location.elementShift);
    }

    public void add(T element)
    {
        append(element);
    }

    public void add(int index, T element)
    {
        insert(index, element);
    }

    public T remove(int index)
    {
        return drop(index);
    }

    public int size()
    {
        return size;
    }

    public void clear()
    {
        this.size = 0;
        this.totalCapacity = 0;

        while(this.head != null)
            this.head.remove();

        RegionSnapshot s0, s1 = snapshotHead;
        while(s1 != null)
        {
            s0 = s1;
            s1 = s0.next;
            s0.next = null;
        }
    }

    void init()
    {
        this.head = this.tail = new WindowNode(new ArraySegment(INITIAL_CAPACITY));

        this.snapshotHead = new RegionSnapshot();
        this.snapshotHead.capacity = INITIAL_CAPACITY;
        this.snapshotHead.size = 0;
        this.snapshotHead.head = this.head;

        this.snapshotTail = this.snapshotHead;
        this.head.snapshot = this.snapshotHead;
    }

    Location locate(int index)
    {
        if(index >= size || snapshotHead == null)
            throw new IndexOutOfBoundsException(index + "");

        Location location = new Location();

        RegionSnapshot snapshot = snapshotHead;

        int counted = 0;
        while((counted + snapshot.size) <= index)
        {
            location.regionShift++;
            counted += snapshot.size;

            while(snapshot.next.head == null) // remove empty nodes
                snapshot.next = snapshot.next.next;

            snapshot = snapshot.next;
        }

        Node node = snapshot.head;
        while((counted + node.size()) <= index)
        {
            location.segmentShift++;
            counted += node.size();

            node = node.next;
        }

        location.elementShift = index - counted;
        location.located = node;

        return location;
    }

    Node fracture(WindowNode node, int index)
    {
        if(index == 0)
            return node.prev;

        WindowNode p = new WindowNode(node.ref, node.start, node.start += index);
        p.linkBefore(node);
        p.ptr = node.ptr;
        p.snapshot = node.snapshot;

        if(p.snapshot.head == node)
            p.snapshot.head = p;

        return p;
    }

    void insert(int index, T element)
    {
        Location location = locate(index);
        Node n = location.located;

        if(n instanceof ResilientCompoundList.WindowNode)
            fracture((WindowNode) n, location.elementShift);

        ElementNode e = new ElementNode(element);

        e.snapshot = n.snapshot;
        e.linkBefore(n);

        if (e.snapshot.head == n)
            e.snapshot.head = e;
        e.snapshot.size++;
        e.snapshot.capacity++;

        size++;
        totalCapacity++;
    }

    T drop(int index)
    {
        Location location = locate(index);
        T old;

        if(location.located instanceof ResilientCompoundList.ElementNode)
        {
            old = location.located.get(0);
            location.located.remove();

            size--;
            totalCapacity--;
        }
        else // node instanceof WindowNode
        {
            WindowNode node = (WindowNode) location.located;

            old = node.get(location.elementShift);

            fracture(node, location.elementShift);

            if(node.size() == 1)
                node.remove();
            else
            {
                node.set(0, null);

                node.start++;
                node.snapshot.size--;
                node.snapshot.capacity--;

                size--;
                totalCapacity--;
            }
        }

        return old;
    }

    void append(T element)
    {
        if(head == null)
            init();
        else if(snapshotTail.full())
            grow();

        if(!tail.append(element)) // escaped
        {
            ElementNode e = new ElementNode(element);

            e.linkAfter(tail);
            e.snapshot = snapshotTail;
            e.snapshot.size++;
            e.snapshot.capacity++;

            totalCapacity++;
        }

        size++;
    }

    void grow()
    {
        int growth = totalCapacity >>> 1;
        try {
            Segment<T> segment = growth < THERSHOLD ? new ArraySegment(growth) : new ResilientSegment(growth);
            WindowNode node = new WindowNode(segment);

            node.linkAfter(tail);

            if(snapshotTail != null && snapshotTail.OOM)
            {
                snapshotTail.capacity += growth;
                snapshotTail.OOM = false;
            }
            else
            {
                newSnapshot();
                snapshotTail.capacity = growth;
                snapshotTail.size = 0;
                snapshotTail.OOM = false;
                snapshotTail.head = node;
            }

            node.snapshot = snapshotTail;

            totalCapacity += growth;
        } catch (OutOfMemoryError oom) {
            // allocation failure
            ElementNode node = new ElementNode();
            node.linkAfter(tail);

            if(snapshotTail.OOM)
                snapshotTail.capacity++;
            else
            {
                newSnapshot();
                snapshotTail.capacity = 1;
                snapshotTail.size = 0;
                snapshotTail.OOM = true;
                snapshotTail.head = node;
            }

            node.snapshot = snapshotTail;

            totalCapacity++;
        }
    }

    final void newSnapshot()
    {
        if(snapshotHead == null)
            snapshotHead = snapshotTail = new RegionSnapshot();
        else
            snapshotTail = (snapshotTail.next = new RegionSnapshot());
    }

    private Node head;

    private Node tail;

    private int size;

    private int totalCapacity;

    private RegionSnapshot snapshotHead;

    private RegionSnapshot snapshotTail;

    public static final int INITIAL_CAPACITY = 10;

    public static final int CHUNK_SIZE = 1024;

    public static final int THERSHOLD = CHUNK_SIZE + (CHUNK_SIZE >>> 1);

    abstract class Node
    {
        abstract T get(int index);

        abstract void set(int index, T element);

        abstract boolean full();

        abstract boolean append(T element);

        abstract int size();

        abstract int capacity();

        void linkBefore(Node node)
        {
            if (node == null)
            {
                head = tail = this;
                return;
            }

            if(node.prev != null)
                node.prev.next = this;

            this.next = node;
            this.prev = node.prev;

            node.prev = this;

            if(node == head)
                head = this;
        }

        void linkAfter(Node node)
        {
            if(node == null)
            {
                head = tail = this;
                return;
            }

            if(node.next != null)
                node.next.prev = this;

            this.next = node.next;
            this.prev = node;

            node.next = this;

            if(node == tail)
                tail = this;
        }

        void remove()
        {
            if(next != null)
                next.prev = prev;
            else
            {
                tail = prev;
                snapshotTail = prev == null ? null : prev.snapshot;
            }

            if(prev != null)
                prev.next = next;
            else
            {
                head = next;
                snapshotHead = next == null ? null : next.snapshot;
            }

            if(snapshot.head == this)
                if(next != null && snapshot == next.snapshot)
                    snapshot.head = next;
                else
                    snapshot.head = null;

            snapshot.size -= this.size();
            snapshot.capacity -= this.capacity();

            snapshot = null;
            prev = null;
            next = null;
        }

        Node prev;

        Node next;

        RegionSnapshot snapshot;
    }

    class ElementNode extends Node
    {
        ElementNode()
        {
        }

        ElementNode(T element)
        {
            this.element = element;
        }

        @Override
        public T get(int index)
        {
            if(index != 0)
                throw new IllegalStateException(new IndexOutOfBoundsException(index + ""));
            return element;
        }

        @Override
        public void set(int index, T element)
        {
            if(index != 0)
                throw new IllegalStateException(new IndexOutOfBoundsException(index + ""));
            this.element = element;
        }

        @Override
        public boolean full()
        {
            return initialized;
        }

        @Override
        public boolean append(T e)
        {
            if(full())
                return false;
            this.element = e;
            snapshot.size++;
            return true;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public int capacity()
        {
            return 1;
        }

        @Override
        public void remove()
        {
            super.remove();
            element = null;
        }

        boolean initialized;

        T element;
    }

    class WindowNode extends Node
    {
        WindowNode(Segment<T> ref)
        {
            this.ref = ref;
            this.start = 0;
            this.end = ref.capacity();
        }

        WindowNode(Segment<T> ref, int start, int end)
        {
            this.ref = ref;
            this.start = start;
            this.end = end;
        }

        @Override
        public T get(int index)
        {
            return ref.get(index + start);
        }

        @Override
        public void set(int index, T element)
        {
            ref.set(index + start, element);
        }

        @Override
        public boolean full()
        {
            return ptr >= end();
        }

        @Override
        public boolean append(T e)
        {
            if(full())
                return false;
            ref.set(ptr++, e);
            snapshot.size++;
            return true;
        }

        @Override
        public int size()
        {
            return Math.min(capacity(), ptr - start);
        }

        @Override
        public int capacity()
        {
            return end() - start;
        }

        @Override
        public void remove()
        {
            super.remove();
            ref = null;
        }

        int end()
        {
            if(end > ref.capacity())
            {
                snapshot.capacity -= (end - ref.capacity());
                end = ref.capacity();
            }
            return end;
        }

        int ptr;

        int start; // include

        int end; // exclude

        Segment<T> ref;
    }

    interface Segment<T>
    {
        int capacity();

        T get(int index);

        void set(int index, T element);
    }

    class ArraySegment implements Segment<T>
    {
        ArraySegment(int capacity)
        {
            this.elements = new Object[capacity];
            totalCapacity += capacity;
        }

        @Override
        public int capacity()
        {
            return elements.length;
        }

        @Override
        public T get(int index)
        {
            try {
                return (T) elements[index];
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void set(int index, T element)
        {
            try {
                elements[index] = element;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        Object[] elements;
    }

    class ResilientSegment implements Segment<T>
    {
        ResilientSegment(int capacity)
        {
            this.chunk = new SoftChunkRef(new ResilientChunk(this, capacity));
        }

        @Override
        public int capacity()
        {
            check();
            return chunk.get().capacity();
        }

        @Override
        public T get(int index)
        {
            check();
            return chunk.get().get(index);
        }

        @Override
        public void set(int index, T element)
        {
            check();
            chunk.get().set(index, element);
        }

        void check()
        {
            if(chunk.get() == null)
                chunk = new StrongChunkRef(new SurvivorChunk(((SoftChunkRef) chunk).survivors.escape()));
        }

        ChunkRef chunk;
    }

    abstract class ChunkRef
    {
        abstract Chunk get();
    }

    class StrongChunkRef extends ChunkRef
    {
        StrongChunkRef(Chunk chunk)
        {
            this.chunk = chunk;
        }

        @Override
        Chunk get()
        {
            return chunk;
        }

        final Chunk chunk;
    }

    class SoftChunkRef extends ChunkRef
    {
        SoftChunkRef(ResilientChunk chunk)
        {
            this.ref = new SoftReference<>(chunk);
            this.survivors = chunk.survivors;
        }

        @Override
        ResilientChunk get()
        {
            return ref.get();
        }

        final SoftReference<ResilientChunk> ref;

        final SurvivorSpace survivors;
    }

    abstract class Chunk
    {
        T get(int index)
        {
            try {
                return (T) chunks()[index / CHUNK_SIZE][index % CHUNK_SIZE];
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        void set(int index, T element)
        {
            try {
                chunks()[index / CHUNK_SIZE][index % CHUNK_SIZE] = element;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        abstract Object[][] chunks();

        abstract int capacity();
    }

    class ArrayChunk extends Chunk
    {
        ArrayChunk(ResilientSegment owner, Object[][] chunks, int capacity)
        {
            this.capacity = capacity;
            this.chunks = chunks;
            this.owner = owner;
        }

        @Override
        Object[][] chunks()
        {
            return chunks;
        }

        @Override
        void set(int index, T element)
        {
            super.set(index, element);
            size = Math.max(size, index + 1);
        }

        @Override
        int capacity()
        {
            return capacity;
        }

        final Object[][] chunks;

        int size;

        final int capacity;

        private final ResilientSegment owner;
    }

    class SurvivorChunk extends Chunk
    {
        SurvivorChunk(SurvivorSpace survivors)
        {
            this.survivors = survivors;
        }

        @Override
        Object[][] chunks()
        {
            return survivors.survivors;
        }

        @Override
        int capacity()
        {
            return survivors.capacity;
        }

        final SurvivorSpace survivors;
    }

    class ResilientChunk extends Chunk
    {
        ResilientChunk(ResilientSegment owner, int capacity)
        {
            this.owner = owner;

            this.capacity = capacity;

            int i = capacity / CHUNK_SIZE;
            int j = capacity % CHUNK_SIZE;
            int l = i + (j == 0 ? 0 : 1);

            this.chunks = new Object[l][];
            for(int k = 0; k < l - 1; k++)
                this.chunks[k] = new Object[CHUNK_SIZE];
            this.chunks[l - 1] = new Object[j == 0 ? CHUNK_SIZE : j];

            this.survivors = new SurvivorSpace(this.chunks.length);

            totalCapacity += capacity;
        }

        @Override
        Object[][] chunks()
        {
            return chunks;
        }

        @Override
        void set(int index, T element)
        {
            super.set(index, element);
            size = Math.max(size, index + 1);

            int chunkIndex = index / CHUNK_SIZE;
            survivors.save(chunkIndex, this.chunks[chunkIndex]);
        }

        @Override
        int capacity()
        {
            return capacity;
        }

//      @Override
//      protected void finalize() throws Throwable
//      {
//          Object[][] escaped = new Object[size / CHUNK_SIZE + ((size % CHUNK_SIZE) == 0 ? 0 : 1)][];
//
//          for(int i = 0; i < escaped.length; i++)
//              escaped[i] = chunks[i];
//
//          int capacity = CHUNK_SIZE * (escaped.length - 1) + escaped[escaped.length - 1].length;
//          owner.chunk = new StrongChunkRef(new ArrayChunk(owner, escaped, capacity));
//
//          // correct tail
//          if(!(tail instanceof ResilientCompoundList.WindowNode))
//              throw new IllegalStateException("Bad type on tail");
//
//          WindowNode wnode = (WindowNode) tail;
//
//          if(!(wnode.ref instanceof ResilientCompoundList.ResilientSegment))
//              throw new IllegalStateException("Bad type on tail segment");
//
//          if(wnode.ref != this.owner)
//              throw new IllegalStateException("Bad segment on tail");
//
//          if(wnode.start >= capacity)
//              wnode.remove();
//          else
//              wnode.end = capacity;
//
//          totalCapacity -= (this.capacity - capacity);
//
//          super.finalize();
//      }

        final int capacity;

        int size = 0;

        final Object[][] chunks;

        private final ResilientSegment owner;

        final SurvivorSpace survivors;
    }

    class SurvivorSpace
    {
        SurvivorSpace(int chunkCount)
        {
            this.survivors = new Object[chunkCount][];
        }

        void save(int index, Object[] chunk)
        {
            survived = Math.max(survived, index);
            survivors[index] = chunk;
        }

        SurvivorSpace escape()
        {
            for(int i = 0; i < survived; i++)
                capacity += survivors[i].length;
            return this;
        }

        int capacity;

        int survived;

        final Object[][] survivors;
    }

    class RegionSnapshot
    {
        boolean full()
        {
            return size == capacity;
        }

        int size;

        int capacity;

        Node head;

        RegionSnapshot next;

        boolean OOM;
    }

    class Location
    {
        int regionShift;

        int segmentShift;

        int elementShift;

        Node located;
    }
}
