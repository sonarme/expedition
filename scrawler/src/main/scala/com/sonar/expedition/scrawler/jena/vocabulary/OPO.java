package com.sonar.expedition.scrawler.jena.vocabulary;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

public class OPO {
    private static Model model = ModelFactory.createDefaultModel();

    public static final String NS = "http://ggg.milanstankovic.org/opo/ns#";

    public static String getURI() {
        return NS;
    }

    public static final Resource NAMESPACE = model.createResource(NS);

    /**
     * The graphical representation of the Agent used to depict him in online
     * systems
     */
    public static final Property avatar = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#avatar");

    /**
     * Current location of the Agent
     */
    public static final Property currentLocation = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#currentLocation");

    /**
     * Current action performed by the Agent
     */
    public static final Property currentAction = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#currentAction");

    /**
     * The Agent that declared the OnlinePresence
     */
    public static final Property declaredBy = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#declaredBy");

    /**
     * The sioc:User account where the OnlinePresence was declared.
     */
    public static final Property declaredOn = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#declaredOn");

    /**
     * The source from which the OnlinePresence is declared (e.g., a mobile
     * device)
     */
    public static final Property publishedFrom = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#publishedFrom");

    /**
     * The OnlinePresence declared by the Agent
     */
    public static final Property declaresOnlinePresence = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#declaresOnlinePresence");

    /**
     * OnlinePresenceComponent included in this OnlinePresence
     */
    public static final Property hasOnlinePresence = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#hasPresenceComponent");

    /**
     * OnlineStatusComponent included in this OnlineStatus
     */
    public static final Property hasStatusComponent = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#hasStatusComponent");

    /**
     * The OnlineStatus that includes this OnlineStatusComponent.
     */
    public static final Property isStatusComponentOf = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#isStatusComponentOf");

    /**
     * The description of the SourceOfPublishing.
     */
    public static final Property sourceDescription = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#sourceDescription");

    /**
     * A message associated with the OnlinePresence, often used in chat programs
     * and social networks as custom title as well as in microblogging as status
     * message.
     */
    public static final Property customMessage = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#customMessage");

    /**
     * A group of people belonging to a space for sharing online psresence data
     * - the intended audience of presence information.
     */
    public static final Property intendedFor = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#intendedFor");

    /**
     * An interst shared by people in the Sharing Space.
     */
    public static final Property commonInterest = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#commonInterest");

    /**
     * The current location of people in the Sharing Space.
     */
    public static final Property currentlyIn = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#currentlyIn");

    /**
     * The location where members of the Sharing Space are based.
     */
    public static final Property basedNear = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#basedNear");

    /**
     * The homepage of school attended by people in the Sharing Space.
     */
    public static Property schoolHomepage = model
            .createProperty("http://ggg.milanstankovic.org/opo/ns#schoolHomepage");
}
