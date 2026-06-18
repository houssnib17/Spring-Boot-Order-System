package com.example.second.service;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import java.util.concurrent.Semaphore;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.ArrayList;
import java.util.List;

@Service
public class OrderService {

    // 🛠️ تحسين 1: استخدام StringRedisTemplate بدلاً من الـ Generic Template
    // لتجنب مشاكل الـ Serialization الخاصة بـ Java واستخدام سلاسل نصية واضحة في Redis Desktop Manager.
    @Autowired(required = false)
    private StringRedisTemplate redisTemplate;

    private final ConcurrentHashMap<String, Object> localCache = new ConcurrentHashMap<>();

    // محاكاة قاعدة البيانات للمخزون
    private final ConcurrentHashMap<Integer, AtomicInteger> inventory = new ConcurrentHashMap<>() {{
        put(101, new AtomicInteger(150));
    }};

    // الـ Versions الخاصة بالقفل المتفائل
    private final ConcurrentHashMap<Integer, AtomicInteger> productVersion = new ConcurrentHashMap<>() {{
        put(101, new AtomicInteger(1));
    }};

    // ==========================================
    // 3. المعالجة غير المتزامنة (Asynchronous Queues)
    // ==========================================
    private final ExecutorService asyncTaskExecutor = Executors.newFixedThreadPool(10);
    private final AtomicInteger processedAsyncTasks = new AtomicInteger(0);

    // ==========================================
    // 5. توزيع الأحمال (Load Distribution)
    // ==========================================
    private final AtomicInteger loadBalancerCounter = new AtomicInteger(0);
    private final AtomicInteger serverACounter = new AtomicInteger(0);
    private final AtomicInteger serverBCounter = new AtomicInteger(0);

    public String routeLoadAndGetServer() {
        int currentRequestNumber = loadBalancerCounter.incrementAndGet();
        if (currentRequestNumber % 2 == 0) {
            serverACounter.incrementAndGet();
            return "Server_A (Production-Node-1)";
        } else {
            serverBCounter.incrementAndGet();
            return "Server_B (Production-Node-2)";
        }
    }

    public int getServerACount() { return serverACounter.get(); }
    public int getServerBCount() { return serverBCounter.get(); }

    // ==========================================
    // 2. إدارة الموارد والتحكم بالسعة (Capacity Control)
    // ==========================================
    private final Semaphore semaphore = new Semaphore(20);

    // ==========================================
    // 7 & 8. الأقفال وسلامة المعاملات (ACID & Concurrency Control)
    // ==========================================
    public boolean createOrderWithDistributedLock(String userId, int productId, int quantity) {
        if (!semaphore.tryAcquire()) {
            return false; // الرد السريع لحماية النظام من الاختناق (Shedding Load)
        }

        String lockKey = "lock::user-order::" + userId;
        boolean acquired = false;
        boolean isRedisOperational = (redisTemplate != null);

        try {
            // 🛠️ تحسين 2: توحيد آلية الحظر واستدعاء الـ DB في بلوك واحد محمي لمنع تكرار الكود (Clean Code)
            if (isRedisOperational) {
                try {
                    Boolean res = redisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", 5, TimeUnit.SECONDS);
                    acquired = (res != null && res);
                } catch (Exception e) {
                    // Fallback: إذا سقط سيرفر Redis فجأة، يتحول تلقائياً للكاش المحلي لإنقاذ الموقف
                    isRedisOperational = false;
                    acquired = localCache.putIfAbsent(lockKey, "LOCKED") == null;
                }
            } else {
                acquired = localCache.putIfAbsent(lockKey, "LOCKED") == null;
            }

            if (acquired) {
                boolean orderCreated = sellProductWithLockingAndTx(productId, quantity);
                if (orderCreated) {
                    triggerAsynchronousTasks(userId, productId);
                }
                return orderCreated;
            }
            return false;

        } finally {
            // 🛠️ تحسين 3: ضمان فك القفل بالشكل الصحيح بناءً على السيرفر المستعمل دون إحداث NullPointerExceptions
            if (acquired) {
                if (isRedisOperational) {
                    try { redisTemplate.delete(lockKey); } catch (Exception e) { localCache.remove(lockKey); }
                } else {
                    localCache.remove(lockKey);
                }
            }
            semaphore.release(); // تحرير المورد
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public boolean sellProductWithLockingAndTx(int productId, int quantity) {
        AtomicInteger stock = inventory.get(productId);
        AtomicInteger version = productVersion.get(productId);

        if (stock == null) return false;

        synchronized (stock) {
            int currentVersion = version.get();
            int currentStock = stock.get();

            if (currentStock < quantity) {
                return false;
            }

            if (version.compareAndSet(currentVersion, currentVersion + 1)) {
                stock.set(currentStock - quantity);
                queueOrderForBatch(productId); // الجرد التجميعي الخلفي
                return true;
            } else {
                throw new RuntimeException("Optimistic Locking Conflict - Version Mismatch");
            }
        }
    }

    private void triggerAsynchronousTasks(String userId, int productId) {
        asyncTaskExecutor.execute(() -> {
            try {
                Thread.sleep(50);
                processedAsyncTasks.incrementAndGet();
                System.out.println("📨 [Async Queue] Invoice generated and notification sent to user: " + userId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public int getProcessedAsyncTasksCount() { return processedAsyncTasks.get(); }
    public int getStock(int productId) { return inventory.getOrDefault(productId, new AtomicInteger(0)).get(); }

    // ==========================================
    // 6. التخزين المؤقت المشترك المحمي بالأقفال (Cache Aside Locking)
    // حل مشكلة الـ Cache Stampede لضمان صمود الكاش تحت الضغط العالي
    // ==========================================
    public String getProductDetailsWithCache(int productId) {
        String cacheKey = "product::" + productId;
        String lockKey = "lock::cache-init::" + productId;
        String cached = null;
        boolean isRedisOperational = (redisTemplate != null);

        // الخطوة الأولى: محاولة القراءة الفورية من الكاش المشترك (Redis) أو المحلي
        try {
            if (isRedisOperational) {
                cached = redisTemplate.opsForValue().get(cacheKey);
            } else {
                cached = (String) localCache.get(cacheKey);
            }
        } catch (Exception e) {
            isRedisOperational = false; // تحويل فوري محلي في حال وجود خلل في اتصال الشبكة بـ Redis
            cached = (String) localCache.get(cacheKey);
        }

        // ⚡ Cache Hit: تم العثور على البيانات، نرجعها فوراً دون المرور بقاعدة البيانات
        if (cached != null) return cached + " (⚡ Cache Hit)";

        // 🐢 Cache Miss: البيانات غير موجودة، نطبق القفل الموزع لمنع اختناق السيرفر
        boolean acquired = false;
        try {
            if (isRedisOperational) {
                try {
                    Boolean res = redisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", 3, TimeUnit.SECONDS);
                    acquired = (res != null && res);
                } catch (Exception e) {
                    isRedisOperational = false;
                    acquired = localCache.putIfAbsent(lockKey, "LOCKED") == null;
                }
            } else {
                acquired = localCache.putIfAbsent(lockKey, "LOCKED") == null;
            }

            if (acquired) {
                // الخيط الفائز بالقفل هو الوحيد المخوّل بضرب قاعدة البيانات ومحاكاة تأخير الـ DB
                try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                String details = "Product_101_Gaming_Laptop";

                // تحديث طبقة الكاش
                try {
                    if (isRedisOperational) {
                        redisTemplate.opsForValue().set(cacheKey, details, 5, TimeUnit.MINUTES);
                    } else {
                        localCache.put(cacheKey, details);
                    }
                } catch (Exception e) {
                    localCache.put(cacheKey, details);
                }
                return details + " (🐢 Cache Miss - DB Fetched via Lock)";
            } else {
                // باقي الخيوط المتزامنة تنتظر قليلاً (50ms) ثم تعيد المحاولة لتجد الكاش قد امتد بالبيانات
                try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return getProductDetailsWithCache(productId); // استدعاء عودي ذكي لقراءة الكاش الجاهز
            }

        } finally {
            if (acquired) {
                if (isRedisOperational) {
                    try { redisTemplate.delete(lockKey); } catch (Exception e) { localCache.remove(lockKey); }
                } else {
                    localCache.remove(lockKey);
                }
            }
        }
    }

    // ==========================================
    // 4. معالجة البيانات على دفعات (Batch Processing)
    // ==========================================
    private final LinkedBlockingQueue<Integer> batchQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger processedBatchCount = new AtomicInteger(0);

    public void queueOrderForBatch(int productId) {
        batchQueue.add(productId);
        if (batchQueue.size() >= 10) {
            processCurrentBatch();
        }
    }

    private synchronized void processCurrentBatch() {
        List<Integer> currentBatch = new ArrayList<>();
        batchQueue.drainTo(currentBatch, 10);
        if (!currentBatch.isEmpty()) {
            processedBatchCount.addAndGet(currentBatch.size());
            System.out.println("🚀 [Background Job] processed a batch of " + currentBatch.size() + " sales statistics.");
        }
    }

    public int getProcessedBatchCount() { return processedBatchCount.get(); }
}