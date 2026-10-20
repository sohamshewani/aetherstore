#include "concurrency/rwlatch.h"
#include <errno.h>
#include <time.h>

void rwlatch_init(rwlatch_t *latch) {
    if (!latch) {
        return;
    }
    
    pthread_mutexattr_t mattr;
    pthread_mutexattr_init(&mattr);
    pthread_mutexattr_settype(&mattr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&latch->mutex, &mattr);
    pthread_mutexattr_destroy(&mattr);

    pthread_cond_init(&latch->read_cv, NULL);
    pthread_cond_init(&latch->write_cv, NULL);

    latch->readers = 0;
    latch->writer = 0;
    latch->waiting_writers = 0;
    latch->writer_active = false;
}

void rwlatch_destroy(rwlatch_t *latch) {
    if (!latch) {
        return;
    }

    pthread_mutex_lock(&latch->mutex);
    while (latch->readers > 0 || latch->writer_active || latch->waiting_writers > 0) {
        pthread_mutex_unlock(&latch->mutex);
        struct timespec ts = {0, 10000000}; // 10ms
        nanosleep(&ts, NULL);
        pthread_mutex_lock(&latch->mutex);
    }

    pthread_cond_destroy(&latch->read_cv);
    pthread_cond_destroy(&latch->write_cv);
    pthread_mutex_unlock(&latch->mutex);
    pthread_mutex_destroy(&latch->mutex);
}

void rwlatch_rdlock(rwlatch_t *latch) {
    if (!latch) {
        return;
    }

    pthread_mutex_lock(&latch->mutex);
    
    // If there are active writers or waiting writers, readers should wait 
    // to prevent writer starvation, depending on policy. Here we give preference
    // to writers if any are waiting.
    while (latch->writer_active || latch->waiting_writers > 0) {
        pthread_cond_wait(&latch->read_cv, &latch->mutex);
    }
    
    latch->readers++;
    
    pthread_mutex_unlock(&latch->mutex);
}

bool rwlatch_try_rdlock(rwlatch_t *latch) {
    if (!latch) {
        return false;
    }

    pthread_mutex_lock(&latch->mutex);

    if (latch->writer_active || latch->waiting_writers > 0) {
        pthread_mutex_unlock(&latch->mutex);
        return false;
    }

    latch->readers++;
    pthread_mutex_unlock(&latch->mutex);
    return true;
}

void rwlatch_wrlock(rwlatch_t *latch) {
    if (!latch) {
        return;
    }

    pthread_mutex_lock(&latch->mutex);

    latch->waiting_writers++;
    
    while (latch->writer_active || latch->readers > 0) {
        pthread_cond_wait(&latch->write_cv, &latch->mutex);
    }

    latch->waiting_writers--;
    latch->writer_active = true;
    latch->writer = pthread_self();

    pthread_mutex_unlock(&latch->mutex);
}

bool rwlatch_try_wrlock(rwlatch_t *latch) {
    if (!latch) {
        return false;
    }

    pthread_mutex_lock(&latch->mutex);

    if (latch->writer_active || latch->readers > 0) {
        pthread_mutex_unlock(&latch->mutex);
        return false;
    }

    latch->writer_active = true;
    latch->writer = pthread_self();

    pthread_mutex_unlock(&latch->mutex);
    return true;
}

void rwlatch_unlock(rwlatch_t *latch) {
    if (!latch) {
        return;
    }

    pthread_mutex_lock(&latch->mutex);

    if (latch->writer_active) {
        if (!pthread_equal(latch->writer, pthread_self())) {
            // Unlocking a write lock from a different thread is undefined, 
            // but we defensively handle state consistency.
        }
        latch->writer_active = false;
        
        // Priority to waiting writers, otherwise broadcast to readers
        if (latch->waiting_writers > 0) {
            pthread_cond_signal(&latch->write_cv);
        } else {
            pthread_cond_broadcast(&latch->read_cv);
        }
    } else if (latch->readers > 0) {
        latch->readers--;
        if (latch->readers == 0 && latch->waiting_writers > 0) {
            pthread_cond_signal(&latch->write_cv);
        }
    } else {
        // Attempting to unlock an already unlocked latch
    }

    pthread_mutex_unlock(&latch->mutex);
}