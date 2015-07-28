package ru.dsi.bgbilling.modules.inet.accounting.quota;

import org.apache.log4j.Logger;
import ru.bitel.bgbilling.common.BGException;
import ru.bitel.bgbilling.kernel.application.server.Lifecycle;
import ru.bitel.bgbilling.kernel.container.managed.ServerContext;
import ru.bitel.bgbilling.kernel.event.Event;
import ru.bitel.bgbilling.kernel.event.EventListener;
import ru.bitel.bgbilling.kernel.event.EventListenerContext;
import ru.bitel.bgbilling.kernel.event.EventProcessor;
import ru.bitel.bgbilling.modules.inet.access.Access;
import ru.bitel.bgbilling.modules.inet.accounting.Accounting;
import ru.bitel.bgbilling.modules.inet.accounting.InetConnectionCallRuntime;
import ru.bitel.bgbilling.modules.inet.accounting.bean.TrafficAmount;
import ru.bitel.bgbilling.modules.inet.accounting.event.InetAccountingEvent;
import ru.bitel.bgbilling.modules.inet.api.common.bean.InetConnection;
import ru.bitel.bgbilling.modules.inet.runtime.InetApplication;
import ru.bitel.bgbilling.modules.inet.runtime.InetServTypeRuntime;
import ru.bitel.bgbilling.modules.inet.runtime.TrafficTypeLinkRuntime;
import ru.bitel.bgbilling.modules.inet.runtime.device.InetDeviceRuntime;
import ru.bitel.bgbilling.server.util.ServerUtils;
import ru.bitel.common.ParameterMap;
import ru.bitel.common.worker.ThreadContextFactory;
import ru.bitel.common.worker.WorkerThread;
import ru.bitel.common.worker.WorkerThreadFactory;
import ru.dsi.bgbilling.modules.inet.accounting.quota.event.MyInetAccountingEvent;
import ru.dsi.bgbilling.modules.inet.accounting.quota.event.QuotaProfileChangedEvent;

import java.beans.ConstructorProperties;
import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: semen
 * Date: 26.08.13
 * Time: 14:28
 * To change this template use File | Settings | File Templates.
 */
@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
public class QuotaCollector implements EventListener<Event>, Lifecycle {
    private static final Logger logger = Logger.getLogger(QuotaCollector.class);
    private static QuotaCollector instance = null;
    private final InetApplication app;
    /*Структура: servId -> nodeId -> QuotaHolder,
     * где
     *  servId - сервис
     *  nodeId - узел тарифного плана, родительский по отношению к линейке квот - обозначает собственно цепочку квот
     *  QuotaHolder - информация о текущей квоте
     */
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>> quotaMap;
    private volatile ScheduledExecutorService worker;
    private volatile boolean running;
    //private final long workerDelay;

    //Последние счетчики трафиков по коннекшенам и типам трафиков
    //connectionId -> trafficType -> (lastTime, lastAmount)
    private final Map<InetConnection, ConcurrentMap<Integer, LastCounter>> lastCounters;


    @ConstructorProperties({"app"})
    public QuotaCollector(InetApplication app){
        QuotaCollector.instance = this;
        this.app = app;
        this.quotaMap = new ConcurrentHashMap<Integer, ConcurrentHashMap<Long, QuotaHolder>>();
        //connectionId -> trafficType -> (lastTime, lastAmount)
        this.lastCounters = Collections.synchronizedMap(new WeakHashMap<InetConnection, ConcurrentMap<Integer, LastCounter>>());
        //new ConcurrentHashMap<Long, ConcurrentMap<Integer, LastCounter>>();
        this.worker = null;
        this.running=false;
    }

    public static QuotaCollector getInstance() {
        return instance;
    }

    private synchronized void initWorkers(){

        if(this.app instanceof Accounting){
            if(this.worker!=null){
                this.worker.shutdown();
                this.worker = null;
            }

            InetDeviceRuntime rootDeviceRuntime = this.app.deviceMap.get(this.app.rootDeviceId);
            ParameterMap config = rootDeviceRuntime.config;
            //Берём из конфига рутового устройства настройки периодических процессов
            //В секундах
            long checkerDelay = config.getLong("custom.quota.check.delay", 60);
            long flushingDelay = config.getLong("custom.quota.flush.delay", 120);

            ThreadContextFactory<ServerContext> threadContextFactory = new ThreadContextFactory<ServerContext>()
            {
                public ServerContext newThreadContext()
                {
                    return new ServerContext(QuotaCollector.this.app.setup, QuotaCollector.this.app.moduleId, 0);
                }
            };

            ThreadFactory threadFactory = new WorkerThreadFactory<ServerContext>("quotawrkr", null, threadContextFactory)
            {
                protected WorkerThread<ServerContext> init(WorkerThread<ServerContext> t)
                {
                    WorkerThread<ServerContext> result = super.init(t);
                    result.setPriority(5);
                    return result;
                }
            };
            this.worker = Executors.newScheduledThreadPool(1, threadFactory);
            this.worker.scheduleWithFixedDelay(new QuotaCheckerWorker(this.app.moduleId, this.quotaMap), checkerDelay, checkerDelay, TimeUnit.SECONDS);
            this.worker.scheduleWithFixedDelay(new QuotaFlushingWorker(this.app.moduleId, this.app.setup, this.quotaMap), flushingDelay, checkerDelay, TimeUnit.SECONDS);
        }
    }

    /**
     * Возвращает текущий профиль квоты для линейки квот в тарифе от ветки @parentTreeNodeId для договора @contractId
     * @return текущий профиль квоты
     */
    public QuotaHolder getQuotaProfile(int servId, long nodeId){

        QuotaHolder quotaHolder = null;
        Map<Long, QuotaHolder> integerQuotaHolderMap = this.quotaMap.get(servId);
        if(integerQuotaHolderMap!=null){
            quotaHolder = integerQuotaHolderMap.get(nodeId);
        }
        return quotaHolder;
    }

    @Override
    public void notify(Event e, EventListenerContext eventListenerContext) throws BGException {

        logger.debug("caught event: "+e);

        if(!this.running){
            logger.error("QuotaCollector is stopped!");
            return;
        }

        if(e instanceof InetAccountingEvent){
            //Отправляем событие обратно в Accounting
            if(this.app instanceof Access){
                EventProcessor.getInstance().publish(new MyInetAccountingEvent((InetAccountingEvent)e));
            }
        }

        if(e instanceof MyInetAccountingEvent){
            if(this.app instanceof Accounting){
                MyInetAccountingEvent event = (MyInetAccountingEvent)e;

                //Берём id сервиса из коннекшена
                InetConnection connection = event.getConnection();
                if(connection==null){return;}//Нет коннекшена? Странно, пропускаем.
                Map<Long, QuotaHolder> integerQuotaHolderMap = this.quotaMap.get(connection.getServId());
                if(integerQuotaHolderMap==null){return;}//Ничего нет - пропускаем
                //Пишем счётчики в lastCounters, но только если есть квоты для этого соединения
                //Берём InetConnection : connectionMapCall[connection.id].connection
                //Казалось бы, у нас уже есть InetConnection (из event.getConnection()), но для WeakHashMap нам нужен реальный объект,
                // полученный из рантайма, чтобы использовать его в качестве слабого ключа
                InetConnectionCallRuntime inetConnectionCallRuntime = ((Accounting) this.app).connectionMapCall.get(connection.getId());
                if(inetConnectionCallRuntime==null){
                    //странно
                    logger.error("connection runtime not found with id="+connection.getId());
                    return;
                }
                InetConnection conn = inetConnectionCallRuntime.connection;
                ConcurrentMap<Integer, LastCounter> connectionLastCounterMap = this.lastCounters.get(conn);
                logger.debug("lastCounters size = "+lastCounters.size()+" connections");
                if(connectionLastCounterMap==null){//для нашего коннекшена нет данных - заводим хэшмэп
                    connectionLastCounterMap = new ConcurrentHashMap<Integer, LastCounter>();
                    synchronized (this.lastCounters){
                        if(this.lastCounters.get(conn)==null){
                            this.lastCounters.put(conn, connectionLastCounterMap);
                        }else{
                            connectionLastCounterMap = this.lastCounters.get(conn);
                        }
                    }
                }

                long now = System.currentTimeMillis();
                Map<Integer,TrafficAmount> counterTraffics = event.getCounterTraffics();
                //logger.debug("counter traffics: "+counterTraffics);
                Map<Integer,TrafficDelta> trafficDeltas = new HashMap<Integer, TrafficDelta>();
                LastCounter lastCounter;
                TrafficAmount trafficAmount;

                ///Берём все типы трафиков для нашего типа сервиса.
                InetServTypeRuntime servType = inetConnectionCallRuntime.inetServRuntime.inetServTypeRef.get();
                TrafficTypeLinkRuntime trafficTypeLink = servType.trafficTypeLinkRuntimeRef.get();

                //if(counterTraffics==null){return;}

                //Перебираем все типы трафиков сервиса и увеличиваем lastCounters.
                //Если в counterTraffics нет какого-то типа трафика, то нужно проинициализировать его 0 и текушим временем
                for (Integer counterTrafficType : trafficTypeLink.counterTrafficTypes) {
                    lastCounter = connectionLastCounterMap.get(counterTrafficType);

                    if(counterTraffics!=null && counterTraffics.get(counterTrafficType)!=null){
                        //Трафик этого типа найден в пакете, обрабатываем
                        trafficAmount = counterTraffics.get(counterTrafficType);
                        //Если для этого типа трафиков ещё нет счётчика - заводим его (атомарно!)
                        if(lastCounter==null){
                            lastCounter = new LastCounter(0,0);
                            connectionLastCounterMap.putIfAbsent(counterTrafficType, lastCounter);
                            lastCounter = connectionLastCounterMap.get(counterTrafficType);
                        }
                        //Начисление
                        //noinspection SynchronizationOnLocalVariableOrMethodParameter
                        synchronized (lastCounter){
                            if(lastCounter.lastTime<=0){
                                lastCounter.lastTime = now;
                                lastCounter.lastAmount = trafficAmount.amount;
                            }else{
                                //Добавляем трафики
                                trafficDeltas.put(
                                        counterTrafficType,
                                        new TrafficDelta(
                                                trafficAmount.amount - lastCounter.lastAmount,
                                                lastCounter.lastTime,
                                                now
                                        ));
                                lastCounter.lastTime = now;
                                lastCounter.lastAmount = trafficAmount.amount;
                            }
                        }
                    }else if(lastCounter==null){
                        //В аккаунтинг-пакете не было такого типа трафика и он ещё не заведён в LastCounters,
                        //поэтому проинициализируем его нулём и текущим временем
                        //Нужно, чтобы обсчёт начинался не со второго апдейта, а с первого - т.е. пустые каунтеры мы заводим на Acct-Start
                        lastCounter = new LastCounter(now,0);
                        connectionLastCounterMap.putIfAbsent(counterTrafficType, lastCounter);
                    }
                }

                long amount;
                QuotaHolder quotaHolder;
                QuotaProfile quota;
                //Перебираем для нашего сервиса все текущие квоты (nodeId, QuotaHolder)
                for (Map.Entry<Long, QuotaHolder> entry : integerQuotaHolderMap.entrySet()) {
                    quotaHolder = entry.getValue();
                    quota = quotaHolder.quota;
                    if(quota==null){continue;}
                    quota.collect(trafficDeltas);
                    quotaHolder.modified = true;
                    if(logger.isDebugEnabled()){
                        logger.debug("["+quota.name+"] servId="+connection.getServId()+" amount consumed = "+quotaHolder.quota.getTotalAmount()+"/"+quotaHolder.quota.quotaSize);
                        StringBuilder sb = new StringBuilder("[");
                        sb.append(quota.name).append("] servId=").append(connection.getServId()).append(" slices = (");
                        boolean first=true;
                        for (Slice slice : quota.getSlices()) {
                            if(!first){
                                sb.append(", ");
                            }
                            sb.append(slice.amount);
                            first=false;
                        }
                        sb.append(")");

                        logger.debug(sb.toString());
                    }
                    //Проверка квоты
                    //Проверяем, только если quotaSize>0 (в противном случае, это самая последняя квота и клиент может висеть там вечно)
                    if(quota.quotaSize>0 && quota.downProfileName!=null){
                        //Если сумма по всем трафикам в слайсах > quotaSize, то опускаемся вниз
                        amount = quota.getTotalAmount();
                        if(amount > quota.quotaSize){
                            synchronized (quotaHolder){
                                quotaHolder.expireTime = System.currentTimeMillis()+quota.sliceCount*quota.slicePeriod;
                                quotaHolder.name = quota.downProfileName;
                                //Удаляем объект квоты, чтобы дальше с ним не работать,
                                // ждём создания нового объекта из тарифа
                                quotaHolder.quota = null;
                            }
                            //Кидаем событие смены квоты
                            EventProcessor.getInstance().publish(new QuotaProfileChangedEvent(this.app.moduleId, quota.downProfileName, connection.getServId(), entry.getKey()));
                            //Кидаем событие на тарификацию
                            //sssTODO - нужно версию 6.0, чтобы реализовать тарификацию по событию
                            logger.info("quota changed for servId="+connection.getServId()+": "+quota.name+" -> " + quota.downProfileName+" ("+amount+"/"+quota.quotaSize+" bytes consumed)");
                        }
                    }
                }
            }
        }

        if(e instanceof QuotaProfileChangedEvent){
            QuotaProfileChangedEvent event = (QuotaProfileChangedEvent)e;
            this.putQuotaName(event.getServId(), event.getNodeId(), event.getQuotaName());
        }
    }

    @Override
    public void start() throws Exception {
        logger.info("starting QuotaCollector");
        Connection con = this.app.setup.getDBConnectionFromPool();
        QuotaManager quotaManager = new QuotaManager(con, this.app.moduleId);

        try{
            if(this.app instanceof Access){
                quotaManager.loadAll(this.quotaMap, false);
                quotaManager.cleanUp();
                //В Access ловим событие InetAccountingEvent и "отфутболиваем" его обратно в Accounting в виде MyInetAccountingEvent
                //трафики считаем в Accounting,
                //а в Access - только отражаем изменения квот (QuotaProfileChangedEvent) для запросов тарификации
                EventProcessor ep = EventProcessor.getInstance();
                ep.addListener(this, InetAccountingEvent.class, app.moduleId, null);
                ep.addListener(this, QuotaProfileChangedEvent.class, app.moduleId, null);
            }else if (this.app instanceof Accounting){
                quotaManager.loadAll(this.quotaMap, true);
                quotaManager.cleanUp();
                EventProcessor ep = EventProcessor.getInstance();
                ep.addListener(this, MyInetAccountingEvent.class, app.moduleId, null);
                this.initWorkers();
            }
        }finally {
            quotaManager.recycle();
            ServerUtils.closeConnection(con);
        }
        this.running = true;
    }

    @Override
    public void stop() throws Exception {
        this.running = false;
        if(this.worker !=null){
            this.worker.shutdown();
            this.worker=null;
        }
        logger.info("stopping QuotaCollector");
    }

    /**
     * Заносим объект квоты в мэпу
     * @param servId
     * @param nodeId
     * @param quotaProfile
     */
    public void putQuota(int servId, Long nodeId, QuotaProfile quotaProfile) {
        if(!this.running){
            logger.error("QuotaCollector is stopped!");
            return;
        }
        if(this.app instanceof Accounting){
            logger.debug("putting quota "+quotaProfile.name+" for servId="+servId+", nodeId="+nodeId);
            ConcurrentHashMap<Long, QuotaHolder> servIdQuotaHolderMap = this.quotaMap.get(servId);
            if(servIdQuotaHolderMap==null){
                servIdQuotaHolderMap = new ConcurrentHashMap<Long, QuotaHolder>(1);
                servIdQuotaHolderMap.put(nodeId, new QuotaHolder(quotaProfile));
                //Кладём в quotaMap непустой servIdQuotaHolderMap, т.к. пустые периодически вычищаются как устаревшие
                servIdQuotaHolderMap = this.quotaMap.putIfAbsent(servId, servIdQuotaHolderMap);//Если внезапно кто-то успел тут появиться, берём его
                //В номральном режиме servIdQuotaHolderMap тут должен быть null. Если нет, то в quotaMap уже кто-то успел внедриться - тогда мы обрабатываем это ниже
            }
            if(servIdQuotaHolderMap!=null){// да, снова проверяем - см предыдущий блок!
                QuotaHolder holder = servIdQuotaHolderMap.get(nodeId);
                if(holder==null){
                    holder = servIdQuotaHolderMap.putIfAbsent(nodeId, new QuotaHolder(quotaProfile));
                }
                if(holder!=null){// да, снова проверяем - см предыдущий блок!
                    synchronized (holder){
                        holder.quota = quotaProfile; //Если была предыдущая - переписываем
                        holder.name = quotaProfile.name;
                        holder.expireTime = System.currentTimeMillis()+quotaProfile.expirePeriod;
                        holder.modified = true;
                    }
                }
            }
        }
    }

    /**
     *
     * @param servId
     * @param nodeId
     * @param name
     */
    private void putQuotaName(int servId, Long nodeId, String name) {
        if(!this.running){
            logger.error("QuotaCollector is stopped!");
            return;
        }
        logger.debug("changing quota "+name+" for servId="+servId+", nodeId="+nodeId);
        ConcurrentHashMap<Long, QuotaHolder> servIdQuotaHolderMap = this.quotaMap.get(servId);
        if(servIdQuotaHolderMap==null){
            servIdQuotaHolderMap = new ConcurrentHashMap<Long, QuotaHolder>(1);
            servIdQuotaHolderMap.put(nodeId, new QuotaHolder(name));
            //Кладём в quotaMap непустой servIdQuotaHolderMap, т.к. пустые периодически вычищаются как устаревшие
            servIdQuotaHolderMap = this.quotaMap.putIfAbsent(servId, servIdQuotaHolderMap);//Если внезапно кто-то успел тут появиться, берём его
            //В номральном режиме servIdQuotaHolderMap тут должен быть null. Если нет, то в quotaMap уже кто-то успел внедриться - тогда мы обрабатываем это ниже
        }
        if(servIdQuotaHolderMap!=null){// да, снова проверяем - см предыдущий блок!
            QuotaHolder holder = servIdQuotaHolderMap.get(nodeId);
            if(holder==null){
                holder = servIdQuotaHolderMap.putIfAbsent(nodeId, new QuotaHolder(name));
            }
            if(holder!=null){// да, снова проверяем - см предыдущий блок!
                synchronized (holder){
                    holder.quota = null; //Если была предыдущая - переписываем
                    holder.name = name;
                    holder.expireTime = -1;
                    holder.modified = true;
                }
            }
        }
    }

    private class LastCounter{
        public volatile long lastTime;
        public volatile long lastAmount;

        private LastCounter(long lastTime, long lastAmount) {
            this.lastTime = lastTime;
            this.lastAmount = lastAmount;
        }
    }
}