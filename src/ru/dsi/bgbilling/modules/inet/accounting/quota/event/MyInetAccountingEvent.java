package ru.dsi.bgbilling.modules.inet.accounting.quota.event;

import ru.bitel.bgbilling.kernel.event.Event;
import ru.bitel.bgbilling.kernel.event.EventType;
import ru.bitel.bgbilling.modules.inet.accounting.bean.TrafficAmount;
import ru.bitel.bgbilling.modules.inet.accounting.event.InetAccountingEvent;
import ru.bitel.bgbilling.modules.inet.api.common.bean.InetConnection;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.Map;

/**
 * Отфутболиваем событие InetAccountingEvent обратно в Accounting
 * Причина - через стандартную шину событий нельзя получать события InetAccountingEvent в том же приложении (app.id),
 * где оно было инициировано. По этому ловим его в Access, затем оборачиваем в MyInetAccountingEvent и кидаем обратно,
 * чтобы поймать уже в Accounting
 */
@EventType(noLocal=true)
@XmlRootElement
public class MyInetAccountingEvent extends Event {   //

    @XmlAttribute
    private final int type;
    private final InetConnection connection;
    private final Map<Integer, TrafficAmount> counterTraffics;

    protected MyInetAccountingEvent(){
        this.type = -1;
        this.connection = null;
        this.counterTraffics = null;
    }

    public MyInetAccountingEvent(InetAccountingEvent event) {
        super(event.getModuleId(), CONTRACT_GLOBAL, 0);
        this.type = event.getType();
        this.connection = event.getConnection();
        this.counterTraffics = event.getCounterTraffics();
    }

    protected void prepareMessage(Message message)
            throws JMSException
    {
        super.prepareMessage(message);

        message.setIntProperty("type", this.type);
        message.setIntProperty("deviceId", this.connection.getDeviceId());
    }

    public InetConnection getConnection()
    {
        return this.connection;
    }

    public int getType()
    {
        return this.type;
    }

    public Map<Integer, TrafficAmount> getCounterTraffics()
    {
        return this.counterTraffics;
    }

    protected void toString(StringBuilder sb)
    {
        sb.append("; type: ");
        sb.append(this.type);
        sb.append("; deviceId: ");
        sb.append(this.connection.getDeviceId());
        sb.append("; connectionId: ");
        sb.append(this.connection.getId());
    }
}
