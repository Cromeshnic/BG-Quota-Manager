package ru.dsi.bgbilling.modules.inet.accounting.quota;

import org.apache.commons.collections.Buffer;
import org.apache.commons.collections.BufferUtils;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import ru.bitel.common.ParameterMap;
import ru.bitel.common.Preferences;

import java.util.*;

/**
 * <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAqMAAAFWCAIAAADSSgMIAAA1zElEQVR4nO3dD0wVZ77/cVSgWillW7RYKSLiHwpN+CExNPX656a2YLS21Wxo1ZW0esWu1nbLXenVe0lrCyaIZPWm1tKN126N6XJtu+pqrXHxzyq6rFXKVtbiChatcqmLigqKML/vMHgc4YAj58D84f2KMcMwzDznOc88n3nmzJnxUQAAgHP5mF0AAADQhUh6AACcjKQHAMDJSHqg57p+/brZRQDQ5Uh6wGRHjhyJjIyUiYMHD44cObLbtltRUREaGioTY8aM8Wvm4+Pj6+urTX/66afdWRgAXYekB0zmSvr6+vrKyspu264r6V1CQkJ27typTXdzYQB0HZIe6G6rVq2Kjo4ODw/Pzs5WdElfUlLy0ksvacts27ZNhtSSxIsWLbp27ZrMKS4uHjdunMyZM2fOpUuXWq2z7W9lTkpKyooVK4YOHRoXF3fs2DFtya1bt44ePTo+Pj43N7eDpHcVRtaTnJycmZk5bNiw6dOny3pkQ2FhYStXrmxv0wAshaQHulVpaWlMTMyZM2f27dsn6XjixIm2Z+9ltN2/f/8tW7YcP348ISFB0loSNDg4OCcnp7y8fN68edOmTdOv0+1vZW2+vr6vv/66bOKFF16YMmWKzLxw4UJAQIAcYezZsyc2NraDpHcVRib69OmzePHiw4cPywFKYGDg5s2b5UCkV69ely9f7rhgAKyApAe61e7duwcOHHj06FGZrqyslKRsm/TvvfeeKzKLiory8/Pz8vLGjBmjzamurvbz87ty5YprnW5/K2t7+OGHb968KTMPHDgwfPhwmZBVTZw4UVtyw4YNBpM+KCiosbFRphctWvTcc89pCwwePFhK3nHBAFgBSQ90t7S0tAceeCAiIuKdd96RBG2b9HPnzk1PT9f/yZIlS/r16zfgFhlknz59uuPfytoef/xxbYHi4mIt1F977bVly5ZpM2WsbzDpo6KiXCV/++23tekhQ4YcOnSo44IBsAKSHuhWFy9evHr1an19/fbt2yXgP//887ZJn5OTM2fOHG3577//ft++fZmZmfoT42fPnm1qanL96Pa3sraYmBhtjivpV65c6Vrztm3bDCa9az1tk77jggGwApIe6FZ5eXkpKSnayfDk5OSPP/64bdJro+2KioqbN28mJSXl5+cX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" />
 */
public class QuotaProfile {

    public final String name; //Имя профиля

    /**
     * Размер квоты в байтах
     * quotaSize предоставляется на slicePeriod*sliceCount секунд
     */
    public final long quotaSize;

    /**
     * Размер слайса в секундах
     */
    public final long slicePeriod;

    /**
     * Количество слайсов в очереди
     */
    public final int sliceCount;

    /**
     * Период в мс, через который квота "устаревает" при отсутствии тарифных запросов, проходящих через соотв. узел ковты
     * Т.е. при тарифном запросе через узел QuotaProfileTariffTreeNode каунтер QuotaHolder.expireTime устанавливается текущим временем + expirePeriod
     */
    public final long expirePeriod;

    /**
     *  имя профиля, куда клиент попадает, если "плохо себя вёл" в нашем профиле
     */
    public final String downProfileName;

    /**
     * Время, когда у клиента истекает penaltyTime с момента попадания в эту квоту.
     * Если penaltyExpiredTime == 0 - значит, никогда.
     */
    public final long penaltyExpiredTime;

    /**
     * Минимальное время, которое клиент должен просидеть в этом профиле, прежде чем его выкинет "наверх".
     * Однако вниз его может перекинуть в любой момент.
     */
    public final long penaltyPeriod;

    /**
     * Упорядоченная карта имён профилей квот, куда мы будем поднимать клиента по исчетении penaltyPeriod:
     * profile.up.xxx1=profileName1
     * profile.up.xxx2=profileName2
     * где xxx1, xxx2 - объёмы трафика, а profileName1, profileName2 - имена профилей
     * Если по истечении penaltyPeriod клиент за последние slicePeriod*sliceCount секунд
     * нахождения в нашем профиле скачал не более xxx1 байт трафика, то он должен быть переведён в профиль profileName1
     */
    private final SortedMap<Long, String> upProfileNames;

    /**
     * Дефолтный профиль для повышения по истечении penaltyPeriod, если не отработали upProfileNames
     * Т.е. клиент вёл себя хорошо, но не настолько хорошо, чтобы попасть в upProfileNames :)
     */
    private final String upProfileName;

    /**
     * Данные о трафике за последние sliceCount периодов по slicePeriod секунд.
     * Организованы в виде очереди. При добавлении нового элемента самый старый удаляется.
     */
    private final Buffer sliceQueue;

    private final String errorString;

    //Типы трафика, учитываемые в данной квоте
    private final Set<Integer> trafficTypes;

    //Время, когда должен закончиться текущий slice
    private volatile long sliceEndTime;

    //Параметры конфигурации, по которым была создана квота
    private final ParameterMap params;

    /**
     * @param params конфиг профиля
     */
    public QuotaProfile(ParameterMap params){
        this(params, -1, null);
    }

    /**
     *
     * @param parameters конфиг профиля
     * @param penaltyExpiredTime указывается при загрузке из базы
     * @param slices начальные данные трафиков по слайсам (может быть null) - используется при загрузке из базы после ребута
     */
    public QuotaProfile(ParameterMap parameters, long penaltyExpiredTime, List<Slice> slices){

        //(боромир.jpg)
        //Нельзя просто так взять и присвоить this.params = parameters ,
        //т.к. в parameters куча лишнего говна, из-за которого не получится сделать new Preferences(parameters.toString())
        // - возникает Exception
        //(new Preferences из строки происходит при загрузке квот из базы)

        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("name", parameters.get("name", ""));
        paramsMap.put("quota.size", String.valueOf(parameters.getInt("quota.size",0)));
        paramsMap.put("slice.period", String.valueOf(parameters.getInt("slice.period",0)));
        paramsMap.put("slice.count", String.valueOf(parameters.getInt("slice.count",0)));
        paramsMap.put("traffic.types", parameters.get("traffic.types",""));
        String profileDown = parameters.get("profile.down", null);
        if(profileDown!=null){
            paramsMap.put("profile.down", profileDown);
        }
        paramsMap.put("expire.period", String.valueOf(parameters.getInt("expire.period", 86400)));
        paramsMap.put("penalty.period", String.valueOf(parameters.getLong("penalty.period", 0)));
        String profileUp = parameters.get("profile.up", null);
        if(profileUp!=null){
            paramsMap.put("profile.up", profileUp);
        }

        for (Map.Entry<String, ParameterMap> entry : parameters.subKeyed("profile.up.").entrySet()) {
            paramsMap.put("profile.up."+entry.getKey(),entry.getValue().get(""));
        }

        this.params = new Preferences(paramsMap);

        StringBuilder sb = new StringBuilder();
        this.name = this.params.get("name", "");

        this.quotaSize = this.params.getInt("quota.size",0);
        if(this.quotaSize<0){sb.append("quota.size<0\n");}

        this.slicePeriod = params.getInt("slice.period",0)*1000;
        if(this.slicePeriod<=0){sb.append("slice.period<=0 or undefined\n");}

        this.sliceCount = params.getInt("slice.count",0);
        if(this.sliceCount<=0){sb.append("slice.count<=0 or undefined\n");}

        this.trafficTypes = new TreeSet<Integer>(params.getIntegerList("traffic.types", Collections.<Integer>emptyList()));
        if(trafficTypes.isEmpty()){sb.append("traffic.types is empty or undefined\n");}

        this.downProfileName = params.get("profile.down", null);

        this.expirePeriod = params.getInt("expire.period", 86400)*1000;

        this.penaltyPeriod = params.getLong("penalty.period", 0)*1000;
        if(this.penaltyPeriod<0){sb.append("penalty.period<0\n");}

        this.upProfileNames = new TreeMap<Long, String>();
        Map<String, ParameterMap> upProfiles = params.subKeyed("profile.up.");
        for (Map.Entry<String, ParameterMap> entry : upProfiles.entrySet()) {
            try {
                this.upProfileNames.put(Long.valueOf(entry.getKey()), entry.getValue().get(""));
            }catch (NumberFormatException ex){
                sb.append("error param format: profile.up.").append(entry.getKey()).append("=").append(entry.getValue().get("")).append("\n");
            }
        }
        this.upProfileName = params.get("profile.up", null);


        this.sliceQueue = BufferUtils.synchronizedBuffer(new CircularFifoBuffer(this.sliceCount));
        this.sliceEndTime = 0;

        if(penaltyExpiredTime>0){
            this.penaltyExpiredTime = penaltyExpiredTime;
        }else{
            if(this.penaltyPeriod>0){
                this.penaltyExpiredTime = System.currentTimeMillis()+this.penaltyPeriod;
            }else{
                this.penaltyExpiredTime = 0;
            }
        }

        //Если указана, заполняем sliceQueue
        if(slices!=null){
            for (Slice slice : slices) {
                this.sliceQueue.add(slice);
                //Запоминаем время прихода последнего загружаемого слайса
                if(slice.endTime>this.sliceEndTime){
                    this.sliceEndTime = slice.endTime;
                }
            }
        }

        this.errorString = sb.toString();
    }

    public ParameterMap getParams(){
        return this.params;
    }


    public String getErrorString() {
        return this.errorString;
    }

    public void collect(Map<Integer,TrafficDelta> trafficDeltas) {
        long now = System.currentTimeMillis();
        //Сначала добавим ячеек
        this.shiftSlices(now);

        TrafficDelta trafficDelta;
        Slice slice;
        long trafficDeltaTimeMillis;
        for (Map.Entry<Integer, TrafficDelta> trafficDeltaEntry : trafficDeltas.entrySet()) {
            if(this.trafficTypes.contains(trafficDeltaEntry.getKey())){//Наш тип трафика - берём
                trafficDelta = trafficDeltaEntry.getValue();

                synchronized(this.sliceQueue) {
                    for (Object o : this.sliceQueue) {
                        slice = (Slice) o;
                        if (trafficDelta.start < slice.endTime) {
                            trafficDeltaTimeMillis = trafficDelta.end - trafficDelta.start;
                            //Избегаем деления на ноль:
                            if(trafficDeltaTimeMillis == 0L){
                                trafficDeltaTimeMillis = 1L;
                            }

                            //Если трафик попал в данный слайс,
                            // то добавляем туда количество трафика,
                            // пропорциональное времени
                            slice.amount.addAndGet(
                                    trafficDelta.amount *
                                            (Math.min(slice.endTime, trafficDelta.end) - Math.max(trafficDelta.start, slice.endTime - this.slicePeriod))
                                            /
                                            trafficDeltaTimeMillis);
                        }
                    }
                }
            }
        }
    }

    public String getUpProfileName(){
        long amount = this.getTotalAmount();
        //SortedMap.entrySet() выдаёт значения в возрастающем порядке ключа, так что всё ок
        for (Map.Entry<Long, String> entry : this.upProfileNames.entrySet()) {
            if(amount<entry.getKey()){
                return entry.getValue();
            }
        }
        return this.upProfileName;
    }

    /**
     * Сдвигаем очередь ячеек на текущее время
     * Слайсы должны сдвигаться только через этот метод!
     * В частности, sliceEndTime изменяем только здесь!
     */
    private synchronized void shiftSlices(long now) {
        while(this.sliceEndTime< now){
            this.sliceEndTime+= this.slicePeriod;
            this.sliceQueue.add(new Slice(0, this.sliceEndTime));
        }
    }

    /**
     * @return общее количество потреблённого трафика в текущей квоте
     */
    public long getTotalAmount(){
        Slice slice;
        long now = System.currentTimeMillis();
        long amount = 0;
        synchronized(this.sliceQueue){
            for (Object o : this.sliceQueue) {
                slice = (Slice)o;
                if(slice.endTime > now - this.sliceCount * this.slicePeriod){
                    //Берём только слайсы, попадающие в текущий период,
                    // т.к. очередь слайсов может быть устаревшей
                    amount+=slice.amount.get();
                }
            }
        }
        return amount;
    }

    /**
     * Для дебага - смотрим очередь трафиков по слайсам
     * @return список трафиков слайсов в байтах/секундах
     */
    public List<Slice> getSlices(){
        List<Slice> result = new ArrayList<Slice>();
        Slice s;
        synchronized(this.sliceQueue) {
            for (Object o : this.sliceQueue) {
                s = (Slice) o;
                result.add(s);
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
