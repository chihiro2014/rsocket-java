package reactor.util.concurrent;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;
import reactor.util.annotation.Nullable;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.function.BiPredicate;

final class MpscLinkedQueue<E> extends AbstractQueue<E> implements BiPredicate<E, E> {
    //private ManyToOneConcurrentLinkedQueue<E> queue = new ManyToOneConcurrentLinkedQueue<>();
    private org.jctools.queues.MpscLinkedQueue<E> queue = org.jctools.queues.MpscLinkedQueue.newMpscLinkedQueue();
    //private MpscUnboundedArrayQueue<E> queue = new MpscUnboundedArrayQueue<>(8);
    
   
    public MpscLinkedQueue() {
    
    }
    
    
    /**
     * {@inheritDoc} <br>
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * Offer is allowed from multiple threads.<br>
     * Offer allocates a new node and:
     * <ol>
     * <li>Swaps it atomically with current producer node (only one producer 'wins')
     * <li>Sets the new node as the node following from the swapped producer node
     * </ol>
     * This works because each producer is guaranteed to 'plant' a new node and link the old node. No 2
     * producers can get the same producer node as part of XCHG guarantee.
     *
     * @see java.util.Queue#offer(Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public final boolean offer(final E e) {
        return queue.offer(e);
    }
    
    /**
     * This is an additional {@link java.util.Queue} extension for
     * {@link java.util.Queue#offer} which allows atomically offer two elements at once.
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * Offer over {@link #test} is allowed from multiple threads.<br>
     * Offer over {@link #test} allocates a two new nodes and:
     * <ol>
     * <li>Swaps them atomically with current producer node (only one producer 'wins')
     * <li>Sets the new nodes as the node following from the swapped producer node
     * </ol>
     * This works because each producer is guaranteed to 'plant' a new node and link the old node. No 2
     * producers can get the same producer node as part of XCHG guarantee.
     *
     * @see java.util.Queue#offer(Object)
     *
     * @param e1 first element to offer
     * @param e2 second element to offer
     *
     * @return indicate whether elements has been successfully offered
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean test(E e1, E e2) {
        queue.offer(e1);
        queue.offer(e2);
        return true;
    }
    
    /**
     * {@inheritDoc} <br>
     * <p>
     * IMPLEMENTATION NOTES:<br>
     * Poll is allowed from a SINGLE thread.<br>
     * Poll reads the next node from the consumerNode and:
     * <ol>
     * <li>If it is null, the queue is assumed empty (though it might not be).
     * <li>If it is not null set it as the consumer node and return it's now evacuated value.
     * </ol>
     * This means the consumerNode.value is always null, which is also the starting point for the queue.
     * Because null values are not allowed to be offered this is the only node with it's value set to null at
     * any one time.
     *
     * @see java.util.Queue#poll()
     */
    @Nullable
    @Override
    public E poll() {
        return queue.poll();
    }
    
    @Nullable
    @Override
    public E peek() {
        return queue.peek();
    }
    
    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }
    
    
    @Override
    public void clear() {
        while (poll() != null && !isEmpty()) { } // NOPMD
    }
    
    @Override
    public int size() {
        return queue.size();
    }
    
    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }
    
    @Override
    public Iterator<E> iterator() {
        return queue.iterator();
    }
}
