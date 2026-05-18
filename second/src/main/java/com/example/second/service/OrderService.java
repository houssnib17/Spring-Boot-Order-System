package com.example.second.service;

import org.springframework.stereotype.Service;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import java.util.List;

@Service
public class OrderService {

    // 1. إدارة الموارد (المتطلب الثاني) - سمافور يحمي السيرفر بحد أقصى 3 عمليات متزامنة
    private final Semaphore semaphore = new Semaphore(3);

    // 2. معالجة الدفعات (المتطلب الرابع) - طابور آمن برمجياً لتجميع طلبات الـ Batch
    private final ConcurrentLinkedQueue<Integer> batchQueue = new ConcurrentLinkedQueue<>();

    // =========================================================================
    // [تابع للمتطلب الأول] معالجة طلب محاكاة التحديد العشوائي للـ Rate Limiter القديم إذا لزم
    // =========================================================================
    public boolean processOrderWithRateLimit(int orderId) {
        // آلية بسيطة للسماح أو الرفض العشوائي لمحاكاة حماية النظام
        return Math.random() > 0.3;
    }

    // =========================================================================
    // [تابع للمتطلب الثاني] التحكم في الطاقة الاستيعابية عبر الـ Semaphore
    // =========================================================================
    public void processOrderWithCapacity(int id) {
        try {
            if (semaphore.tryAcquire()) {
                System.out.println("📥 [Capacity Control] الخيط " + Thread.currentThread().getName() + " حجز مقعداً وبدأ معالجة الطلب: " + id);
                Thread.sleep(2000); // محاكاة عملية معالجة ثقيلة تستغرق ثانيتين
                System.out.println("✅ [Capacity Control] الخيط " + Thread.currentThread().getName() + " أنهى العمل وحرر المقعد للطلب: " + id);
                semaphore.release();
            } else {
                System.out.println("⚠️ [Capacity Control] السيرفر ممتلئ! تم رفض معالجة الطلب رقم: " + id + " مؤقتاً لحماية الموارد.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // =========================================================================
    // [تابع للمتطلب الثالث] المعالجة غير المتزامنة للإشعارات والفواتير خلف الكواليس
    // =========================================================================
    public void generateInvoiceAndNotify(int id) {
        // إطلاق خيط خلفي فوري منفصل تماماً عن خيط الـ HTTP الرئيسي للمستخدم
        new Thread(() -> {
            try {
                System.out.println("📬 [Async Background Thread] بدأ تجميع بيانات الفاتورة للطلب رقم: " + id);
                Thread.sleep(3000); // محاكاة عملية توليد وتصدير PDF وإرسال إيميل بطيء
                System.out.println("📬 [Async Background Thread] 🟢 تم إرسال الفاتورة والإشعار بنجاح للمستخدم صاحب الطلب: " + id);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    // =========================================================================
    // [تابع للمتطلب الرابع] تجميع ومعالجة البيانات على دفعات (Batch Processing)
    // =========================================================================

    // الدالة المسؤولة عن استقبال الطلبات المفردة ووضعها في الطابور المؤقت
    public void queueOrderForBatch(int id) {
        batchQueue.add(id);
        System.out.println("📥 [Batch Queue] تم إدراج الطلب الفرعي رقم: " + id + " داخل طابور الانتظار المؤقت.");

        // إذا وصل حجم الطابور التجميعي إلى 10 طلبات، نقوم بمعالجتها فوراً كـ Batch دفعة واحدة
        if (batchQueue.size() >= 10) {
            processCurrentBatch();
        }
    }

    // الدالة الخلفية التي تقوم بدمج الـ 10 طلبات ومعالجتها دفعة واحدة لتقليص زمن الاستهلاك
    private synchronized void processCurrentBatch() {
        if (batchQueue.size() < 10) return; // حماية إضافية من الوصول المتزامن المزدوج

        List<Integer> currentBatch = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Integer id = batchQueue.poll();
            if (id != null) {
                currentBatch.add(id);
            }
        }

        System.out.println("\n🔥 [⚡ Batch Engine ⚡] تم تفعيل المعالجة الدفعية الذكية!");
        System.out.println("🔄 جاري دمج وعمل Bulk Insert/Process للطلبات التالية معاً: " + currentBatch);
        try {
            Thread.sleep(200); // محاكاة معالجة الـ 10 طلبات مدمجة دفعة واحدة في نفس الوقت!
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        System.out.println("✨ [⚡ Batch Engine ⚡] نجحت معالجة الدفعة بالكامل ووفرنا 95% من الوقت المستهلك!\n");
    }
}