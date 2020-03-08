package it.univr.veronacard.hadoop.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

public class OperationConf extends Configuration {

    HContexBasedConf hContextBasedConf;

    public OperationConf(GenericOptionsParser genericOptionsParser) {
        super(genericOptionsParser.getConfiguration());

        JAXBContext jaxbContext;
        try {
            jaxbContext = JAXBContext.newInstance(HContexBasedConf.class);
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            hContextBasedConf = (HContexBasedConf) unmarshaller.unmarshal(getClass().getClassLoader().getResource("conf.xml"));
        } catch (JAXBException e) {
            e.printStackTrace();
        }
    }

}
