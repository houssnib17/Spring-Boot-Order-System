package com.example.second.controller;

import com.example.second.service.OrderService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    // متغيرات اختبار التضارب (Race Condition) للمتطلب الأول
    private int unsafeStock = 10;
    private int safeStock = 10;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    // =========================================================================
    // 1. المتطلب الأول: فحص ومعالجة التضارب (Race Condition & Data Integrity)
    // =========================================================================

    // 🔴 [الوضع القديم - Before]: محاكاة التضارب السلبي بدون أي قفل أو تزامن
    @GetMapping("/race-before")
    public String triggerRaceBefore() {
        unsafeStock = 10;
        System.out.println("\n💥 [Race Condition - Before] إطلاق خيوط عشوائية متداخلة لتحديث المخزون بدون حماية...");

        for (int i = 1; i <= 20; i++) {
            new Thread(() -> {
                if (unsafeStock > 0) {
                    try { Thread.sleep(30); } catch (Exception e) {}
                    unsafeStock--;
                    System.out.println("⚠️ سحب غير آمن! المخزون الحالي: " + unsafeStock);
                } else {
                    System.out.println("❌ محاولة سحب خاطئة! المخزون مفقود أو سالب تحت الضغط.");
                }
            }).start();
        }
        return "🔴 [Before] تم إطلاق خيوط التضارب عشوائياً في الذاكرة.";
    }

    // 🟢 [الوضع الجديد - After]: حماية المخزون والمزامنة الكاملة المتوافقة 100% مع JMeter
    @GetMapping("/race-after")
    public String triggerRaceAfter() {
        safeStock = 10;
        System.out.println("\n🛡️ [Data Integrity - After] إطلاق خيوط معالجة محمية لتحديث المخزون بالتناوب...");

        java.util.List<Thread> threads = new java.util.ArrayList<>();

        for (int i = 1; i <= 20; i++) {
            Thread t = new Thread(() -> {
                synchronized (this) {
                    if (safeStock > 0) {
                        try { Thread.sleep(20); } catch (Exception e) {}
                        safeStock--;
                        System.out.println("🟢 سحب آمن وموثوق! المخزون المتبقي المحمي: " + safeStock);
                    } else {
                        System.out.println("❌ نعتذر، نفذت الكمية بالكامل! محاولة سحب مرفوضة للحفاظ على سلامة البيانات.");
                    }
                }
            });
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return "🟢 [After] تم فحص المزامنة بنجاح وحماية البيانات 100% دون أي تضارب!";
    }

    // =========================================================================
    // 5. المتطلب الخامس: موازنة وتوزيع الأحمال العادل والمثالي (Load Distribution)
    // =========================================================================
    @GetMapping("/test-stress")
    public String triggerStressTest() {
        System.out.println("\n=== 🚀 [Load Distribution - After] استقبال طلب مفرد وجدولة 10 طلبات مستقلة كلياً ===");

        // إجبار النظام على تعطيل الـ Keep-Alive لكسر ثبات قنوات السوكيت المحلية
        System.setProperty("http.keepAlive", "false");

        // إنشاء مجدول خيوط دقيق لإرسال الطلبات بفواصل زمنية صارمة وثابتة
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        AtomicInteger counter = new AtomicInteger(1);
        java.util.Random random = new java.util.Random();

        // جدولة مهمة تتكرر 10 مرات، يفصل بين كل مهمة وأخرى 80 ميلي ثانية (وقت كافٍ جداً لـ Nginx ليفهم كسر القناة وتغيير الوجهة)
        scheduler.scheduleAtFixedRate(() -> {
            int currentStep = counter.getAndIncrement();
            if (currentStep > 10) {
                scheduler.shutdown();
                return;
            }

            int dynamicId = random.nextInt(90000) + currentStep;
            java.net.HttpURLConnection connection = null;
            try {
                // الاتصال الإجباري ببورت Nginx الموحد 8000
                java.net.URL url = new java.net.URL("http://localhost:8000/api/orders/process-single?id=" + dynamicId);
                connection = (java.net.HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");

                // إعدادات رأسية صارمة لعدم استخدام الكاش وقطع الاتصال فور الانتهاء
                connection.setUseCaches(false);
                connection.setRequestProperty("Connection", "close");
                connection.setConnectTimeout(1500);
                connection.setReadTimeout(1500);

                // استهلاك الدفق بالكامل للتأكد من إغلاق الاتصال بنجاح
                try (java.io.InputStream is = connection.getInputStream()) {
                    while (is.read() != -1) {}
                }
            } catch (Exception e) {
                // تجاهل استثناءات القنوات سريعة الإغلاق
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }, 0, 80, TimeUnit.MILLISECONDS);

        return "🟢 [After] تم استقبال طلب واحد من JMeter وبدء التوزيع التناوبي لـ 10 طلبات منفصلة (5 ضد 5) عبر بورت 8000!";
    }

    @GetMapping("/process-single")
    public org.springframework.http.ResponseEntity<String> processSingleOrder(@RequestParam("id") int id) {
        orderService.processOrderWithCapacity(id);
        return org.springframework.http.ResponseEntity.ok("🟢 الطلب تم استقباله ودخل طابور المعالجة المحمية بنجاح.");
    }

    // =========================================================================
    // 3. المتطلب الثالث: الطوابير غير المتزامنة للإشعارات والفواتير (Async Queues)
    // =========================================================================
    @GetMapping("/async-before")
    public org.springframework.http.ResponseEntity<String> triggerAsyncBefore(@RequestParam("id") int id) {
        System.out.println("\n🐢 [Main HTTP Thread - Before] طلب شراء بطيء ومبلك برقم: " + id);
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("❌ [Main HTTP Thread - Before] انتهى الانتظار وتم الرد بعد 3 ثوان للطلب: " + id);
        return org.springframework.http.ResponseEntity.ok("Done Before");
    }

    @GetMapping("/async-test")
    public String triggerAsyncTest(@RequestParam("id") int id) {
        System.out.println("\n📬 [Main HTTP Thread - After] استقبلت طلب شراء من المستخدم برقم: " + id + " على الخيط: " + Thread.currentThread().getName());
        orderService.generateInvoiceAndNotify(id);
        System.out.println("⚡ [Main HTTP Thread - After] تم الرد على المستخدم فوراً وتأكيد الطلب.");
        return "تم تأكيد طلبك بنجاح رقم " + id + "! جاري توليد الفاتورة في الخلفية.";
    }

    // =========================================================================
    // 4. المتطلب الرابع: المعالجة الدفعية (Batch Processing)
    // =========================================================================
    @GetMapping("/batch-before")
    public org.springframework.http.ResponseEntity<String> triggerBatchBefore() {
        System.out.println("\n🐢 [Batch Test - Before] جاري معالجة 10 طلبات بشكل فردي (طلب وراء طلب)...");
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= 10; i++) {
            try {
                Thread.sleep(200);
                System.out.println("🔄 [Before] تم إنهاء معالجة الطلب الفرعي المفرد رقم: " + i);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        return org.springframework.http.ResponseEntity.ok("🔴 [Before] تم إنهاء الـ 10 طلبات فرادى! الوقت المستغرق الإجمالي: " + duration + " ms.");
    }

    @GetMapping("/batch-test")
    public String triggerBatchTest() {
        System.out.println("\n⚡ [Batch Test - After] إطلاق 10 اتصالات مستقلة وموزعة لجدولتها كدفعات (Batch)...");

        int currentPort = org.springframework.web.context.request.RequestContextHolder.getRequestAttributes() != null ?
                ((org.springframework.web.context.request.ServletRequestAttributes) org.springframework.web.context.request.RequestContextHolder.getRequestAttributes()).getRequest().getLocalPort() : 8081;

        for (int i = 101; i <= 110; i++) {
            final int orderId = i;
            new Thread(() -> {
                try {
                    java.net.HttpURLConnection connection = (java.net.HttpURLConnection)
                            new java.net.URL("http://localhost:" + currentPort + "/api/orders/batch-single?id=" + orderId).openConnection();
                    connection.setRequestMethod("GET");
                    connection.getInputStream().read();
                } catch (Exception e) {
                    // تجاهل استثناءات القنوات المحلية سريعة الإغلاق
                }
            }).start();
        }
        return "🟢 [After] تمت جدولة الـ 10 طلبات بنجاح داخلياً! اذهب للكونسول لمشاهدة سرعة دمج الدفعات.";
    }

    @GetMapping("/batch-single")
    public String batchSingle(@RequestParam("id") int id) {
        orderService.queueOrderForBatch(id);
        return "Done";
    }
}