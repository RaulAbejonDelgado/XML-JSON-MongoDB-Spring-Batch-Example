package com.bilbomatica.demo.batch;

import com.bilbomatica.demo.batch.pojo.Person;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


@Configuration
@EnableBatchProcessing
public class JobConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    private MongoTemplate mongoTemplate;

    public static int PRETTY_PRINT_INDENT_FACTOR = 4;

    private String dinamicSlash = "//";


    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .tasklet(new Tasklet() {

                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

                        //File xmlDocPath =  Paths.get(getFilePath());
                        File xmlDocPath =  getFilePath();
                        File[] archivos = new File[xmlDocPath.listFiles().length];

                        archivos = listarFicherosPorCarpeta(xmlDocPath);
                        ArrayList<Person> personas = new ArrayList<>();
                        for (File a : archivos) {
                            personas.add((Person) processXML2Object(a));
                        }
                        insertToMongo(personas);

                         //insertToMongo(json);
                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }
    
    public Step step2(){
        return stepBuilderFactory.get("step2")
            .tasklet(new Tasklet(){
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception{
                
                // checks if our collection exists
                Boolean doesexist = mongoTemplate.collectionExists("foo");
                System.out.println("Status of collection returns :::::::::::::::::::::" + doesexist);
                
                // show all DBObjects in foo collection
                DBCursor alldocs = mongoTemplate.getCollection("foo").find();
                List<DBObject> dbarray = alldocs.toArray();
                System.out.println("list of db objects returns:::::::::::::::::::::" + dbarray);
                
                // execute the three methods we defined for querying the foo collection
                String result = doCollect();
                String resultTwo = doCollectTwo();
                String resultThree = doCollectThree();

                System.out.println(" RESULT:::::::::::::::::::::" + result);

                System.out.println(" RESULT:::::::::::::::::::::" + resultTwo);
//
                System.out.println(" RESULT:::::::::::::::::::::" + resultThree);
                
                
               
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    // this will return the id of the policy that has a specific style
    public String doCollect(){

        Query query = new Query();
        query.addCriteria(Criteria.where("nombre").is("Raul"));
        String result = mongoTemplate.findOne(query, Person.class, "foo").toString();
        return result;

    }

    // this will return all Value elements (however there is only one).
    public String doCollectTwo(){

        Query query = new Query();
        query.addCriteria(Criteria.where("nombre").is("drohne"));
        String result = mongoTemplate.find(query, Person.class, "foo").toString();

        return result;
    }

    // searches for policy with specific id and status date. includes only fields title and description within Value element.
    public String doCollectThree(){

        Query query = new Query();
        query.addCriteria(Criteria.where("selfId").gt(1));
        String result = mongoTemplate.find(query, Person.class, "foo").toString();

        return result;
    }

    // our batch job
    @Bean
    public Job xmlToJsonToMongo() {
        return jobBuilderFactory.get("XML_Processor")
                .start(step1())
                .next(step2())
                .build();
    }

    // takes a parameter of xml path and returns json as a string
    private Object processXML2Object(File xmlDocPath) throws JSONException, JAXBException {

        JAXBContext jaxbContext = JAXBContext.newInstance(Person.class);
        Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();
        Person pe = (Person) jaxbUnmarshaller.unmarshal(xmlDocPath);
        
        return pe;
    }
    
    // no parameter method for creating the path to our xml file
    private File getFilePath() throws JAXBException {

        File directorio = null;

        directorio = new File(System.getProperty("user.dir") + dinamicSlash + "personas" + dinamicSlash );

        return directorio;
    }
    
    // inserts to our mongodb
    private void insertToMongo(ArrayList<Person> objetos){
        //Document doc = Document.parse(jsonString);
        //Person p = (Person) jsonString;
        for(Person p : objetos){
            mongoTemplate.insert(p, "foo");
        }
    }

    private static File[] listarFicherosPorCarpeta(File carpeta) {
        File[] archivos = new File[carpeta.listFiles().length];
        int contador = 0;
        for (final File ficheroEntrada : carpeta.listFiles()) {
            if (ficheroEntrada.isDirectory()) {
                listarFicherosPorCarpeta(ficheroEntrada);
            } else {
                if(contador <  carpeta.listFiles().length){
                    archivos[contador] =  ficheroEntrada;
                    contador++;
                }
                System.out.println(ficheroEntrada.getName());
            }
        }
        return archivos;
    }
}    
