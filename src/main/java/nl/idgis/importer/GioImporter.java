package nl.idgis.importer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;

@Component
public class GioImporter {

    private final JdbcTemplate jdbcTemplate;

    private final String inputFile;
    private final String gioName;
    private final String regelingExpression;

    @Autowired
    public GioImporter(JdbcTemplate jdbcTemplate, String inputFile, String gioName, String regelingExpression) {
        this.jdbcTemplate = jdbcTemplate;
        this.inputFile = inputFile;
        this.gioName = gioName;
        this.regelingExpression = regelingExpression;
    }

    public void importGio() {
        File file = new File(inputFile);
        if (!file.exists()) {
            throw new IllegalArgumentException("Het GIO bestand op de volgende locatie kan niet gevonden worden: " + file.getAbsolutePath());
        }

        // Haal regelingId op
        int regelingVersieId = getRegelingVersieId(regelingExpression);
        int regelingId = getRegelingId(regelingVersieId);
        int eindverantwoordelijkeId = getEindverantwoordelijkeId(regelingVersieId);
        int makerId = getMakerId(regelingVersieId);
        String eindverantwoordelijke = getEindverantwoordelijke(eindverantwoordelijkeId);

        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            dbf.setNamespaceAware(true);
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(file);

            XPath xPath = new XPathBuilder()
                .xmlns("basisgeo", "http://www.geostandaarden.nl/basisgeometrie/1.0")
                .xmlns("geo", "https://standaarden.overheid.nl/stop/imop/geo/")
                .xmlns("gio", "https://standaarden.overheid.nl/stop/imop/gio/")
                .xmlns("gml", "http://www.opengis.net/gml/3.2")
                .build();

            NodeList locaties = (NodeList) xPath.compile("//geo:Locatie").evaluate(doc, XPathConstants.NODESET);
            List<Integer> locatieIds = new ArrayList<>();
            for (int i = 0; i < locaties.getLength(); i++) {
                Node locatie = locaties.item(i);
                Node naam = (Node) xPath.compile("./geo:naam").evaluate(locatie, XPathConstants.NODE);
                Node id = (Node) xPath.compile(".//basisgeo:id").evaluate(locatie, XPathConstants.NODE);
                Node gmlNode = (Node) xPath.compile(".//*[@gml:id='id-" + id.getTextContent() + "']").evaluate(locatie, XPathConstants.NODE);

                // Geometrie + Locatie
                System.out.printf("Bezig met verwerken van locatie '%s' (%d/%d)%n", naam.getTextContent(), i + 1, locaties.getLength());
                if (!geometryExists(id.getTextContent())) {
                    int geometrieId = insertGeometry(id.getTextContent(), naam.getTextContent(), getGml(gmlNode));
                    String geometryType = getGeometryType(geometrieId);
                    locatieIds.add(insertLocatie(naam.getTextContent(), LocalDate.now(), regelingId, geometryType, geometrieId, eindverantwoordelijke));
                } else {
                    int geometrieId = getGeometrieId(id.getTextContent());
                    locatieIds.add(getLocatieId(geometrieId));
                }
            }

            // Groep locatie
            System.out.println("Bezig met het maken van de groepslocatie");
            String geometryType = getLocatieGeometryType(locatieIds.get(0));
            int locatieGroepId = insertLocatie(gioName, LocalDate.now(), regelingId, geometryType, eindverantwoordelijke);

            locatieIds.forEach(locatieId -> linkLocatieToGroep(locatieId, locatieGroepId));

            // Informatieobjectversie
            System.out.println("Bezig met het verwerken van de GIO");
            Node frbrWork = (Node) xPath.compile("//geo:FRBRWork").evaluate(doc, XPathConstants.NODE);
            Node frbrExpression = (Node) xPath.compile("//geo:FRBRExpression").evaluate(doc, XPathConstants.NODE);
            Node achtergrondVerwijzingNode = (Node) xPath.compile("//gio:achtergrondVerwijzing").evaluate(doc, XPathConstants.NODE);
            Node achtergrondActualiteitNode = (Node) xPath.compile("//gio:achtergrondActualiteit").evaluate(doc, XPathConstants.NODE);
            Node nauwkeurigheidNode = (Node) xPath.compile("//gio:nauwkeurigheid").evaluate(doc, XPathConstants.NODE);

            String achtergrondVerwijzing = achtergrondVerwijzingNode != null ? achtergrondVerwijzingNode.getTextContent() : null;
            String achtergrondActualiteit = achtergrondActualiteitNode != null ? achtergrondActualiteitNode.getTextContent() : null;
            String nauwkeurigheid = nauwkeurigheidNode != null ? nauwkeurigheidNode.getTextContent() : null;

            insertInformatieObjectVersie(frbrWork.getTextContent(), frbrExpression.getTextContent(), regelingId, eindverantwoordelijkeId, makerId,
                    gioName, achtergrondVerwijzing, achtergrondActualiteit, nauwkeurigheid, locatieGroepId);
        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    private String getGml(Node gmlNode) {
        try (StringWriter sw = new StringWriter()) {
            TransformerFactory tf = TransformerFactory.newInstance();
            tf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            Transformer t = tf.newTransformer();
            t.setOutputProperty(OutputKeys.INDENT, "yes");
            t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            t.transform(new DOMSource(gmlNode), new StreamResult(sw));

            return sw.toString();
        } catch (IOException | TransformerException e) {
            throw new IllegalStateException("GML kon niet geparsed worden", e);
        }
    }

    private int getRegelingVersieId(String expressionId) {
        String sql = "SELECT id FROM bzk.regelingversie WHERE frbr_expression = ?";
        String errorMessage = "Expression ID niet gevonden in de database: " + expressionId;

        Integer regelingVersieId = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setString(1, expressionId),
                rs -> {
                    if (rs.next()) {
                        return rs.getInt("id");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (regelingVersieId == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return regelingVersieId;
    }

    private int getRegelingId(int regelingVersieId) {
        String sql =
                "SELECT r.id FROM bzk.regeling r " +
                "JOIN bzk.regelingversie rv ON rv.regeling_id = r.id " +
                "WHERE rv.id = ?";
        String errorMessage = "Regeling niet gevonden in de database met regelingversie id: " + regelingVersieId;

        Integer regelingId = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, regelingVersieId),
                rs -> {
                    if (rs.next()) {
                        return rs.getInt("id");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (regelingId == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return regelingId;
    }

    private int getEindverantwoordelijkeId(int regelingVersieId) {
        String sql = "SELECT eindverantwoordelijke_id FROM bzk.regelingversie WHERE id = ?";
        String errorMessage = "Eindverantwoordelijke niet gevonden in de database";

        Integer eindverantwoordelijke = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, regelingVersieId),
                rs -> {
                    if (rs.next()) {
                        return rs.getInt("eindverantwoordelijke_id");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (eindverantwoordelijke == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return eindverantwoordelijke;
    }

    private String getEindverantwoordelijke(int id) {
        String sql = "SELECT stop_id FROM public.stop_waarde WHERE id = ?";
        String errorMessage = "Eindverantwoordelijke niet gevonden in de database";

        String eindverantwoordelijke = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, id),
                rs -> {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (eindverantwoordelijke == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return eindverantwoordelijke.substring(eindverantwoordelijke.lastIndexOf('/') + 1);
    }

    private int getMakerId(int regelingVersieId) {
        String sql = "SELECT maker_id FROM bzk.regelingversie WHERE id = ?";
        String errorMessage = "Maker niet gevonden in de database";

        Integer maker = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, regelingVersieId),
                rs -> {
                    if (rs.next()) {
                        return rs.getInt("maker_id");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (maker == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return maker;
    }

    private boolean geometryExists(String gmlId) {
        String sql = "SELECT * FROM bzk.geometrie WHERE geometrie_id = ?";
        String errorMessage = "Kon niet bepalen of de geometrie met id '" + gmlId + "' al bestaat";

        return jdbcTemplate.query(
                sql,
                ps -> ps.setString(1, gmlId),
                (rs, rowNum) -> rs.getInt(1)
        ).stream().findFirst().isPresent();
    }

    private int insertGeometry(String gmlId, String naam, String gml) {
        String sql =
                "INSERT INTO bzk.geometrie (naam, geometrie_id, geometrie) " +
                "VALUES (?, ?, ST_GEOMFROMGML(?, 28992)) " +
                "RETURNING id";
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(
                conn -> {
                    PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, naam);
                    ps.setString(2, gmlId);
                    ps.setString(3, gml);

                    return ps;
                },
                keyHolder
        );

        return Optional.ofNullable(keyHolder.getKey())
            .map(Number::intValue)
            .orElseThrow(() -> new IllegalStateException("Er ging iets mis bij het inserten van de geometrie " + gmlId));
    }

    private int getGeometrieId(String gmlId) {
        String sql = "SELECT id FROM bzk.geometrie WHERE geometrie_id = ?";
        String errorMessage = "Kon de id van de geometrie '" + gmlId + "' niet ophalen";

        Integer id = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setString(1, gmlId),
                rs -> {
                    if (rs.next()) {
                        return rs.getInt("id");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (id == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return id;
    }

    private String getGeometryType(int geometrieId) {
        String sql = "SELECT ST_GEOMETRYTYPE(geometrie) geom_type FROM bzk.geometrie WHERE id = ?";
        String errorMessage = "Kon het type van de geometrie niet bepalen";

        String geometryType = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, geometrieId),
                rs -> {
                    if (rs.next()) {
                        String type = rs.getString("geom_type").toLowerCase();
                        if (type.contains("polygon")) {
                            return "vlak";
                        } else if (type.contains("line")) {
                            return "lijn";
                        } else if (type.contains("point")) {
                            return "punt";
                        }
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (geometryType == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return geometryType;
    }

    private String getLocatieGeometryType(int locatieId) {
        String sql = "SELECT geometrietype FROM bzk.locatie WHERE id = ?";
        String errorMessage = "Kon het geometrietype van de locatie niet bepalen";

        String geometryType = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, locatieId),
                rs -> {
                    if (rs.next()) {
                        return rs.getString("geometrietype");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (geometryType == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return geometryType;
    }

    private int getLocatieId(int geometrieId) {
        String sql = "SELECT id FROM bzk.locatie WHERE geometrie_id = ?";
        String errorMessage = "Kon de id van de locatie niet ophalen";

        Integer id = jdbcTemplate.query(
                conn -> conn.prepareStatement(sql),
                ps -> ps.setInt(1, geometrieId),
                rs -> {
                    if (rs.next()) {
                        return rs.getInt("id");
                    }
                    throw new IllegalArgumentException(errorMessage);
                }
        );

        if (id == null) {
            throw new IllegalArgumentException(errorMessage);
        }

        return id;
    }

    private int insertLocatie(String name, LocalDate dateStart, int regelingId, String geometryType, int geometryId, String bgCode) {
        String sql =
                "INSERT INTO bzk.locatie (naam, datum_begin, ind_groep_jn, regeling_id, geometrietype, geometrie_id, identificatie) " +
                "VALUES (?, ?, false, ?, ?, ?, ?) " +
                "RETURNING id";
        KeyHolder keyHolder = new GeneratedKeyHolder();

        String objectType = "vlak".equals(geometryType) ? "gebied" : geometryType;
        String identificatie = "nl.imow-" + bgCode + "." + objectType + "." + UUID.randomUUID().toString().replace("-", "").toLowerCase();
        jdbcTemplate.update(
                conn -> {
                    PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, name);
                    ps.setDate(2, Date.valueOf(dateStart));
                    ps.setInt(3, regelingId);
                    ps.setString(4, geometryType);
                    ps.setInt(5, geometryId);
                    ps.setString(6, identificatie);

                    return ps;
                },
                keyHolder
        );

        return Optional.ofNullable(keyHolder.getKey())
            .map(Number::intValue)
            .orElseThrow(() -> new IllegalStateException("Er ging iets mis bij het inserten van de locatie"));
    }

    private int insertLocatie(String name, LocalDate dateStart, int regelingId, String geometryType, String bgCode) {
        String sql =
                "INSERT INTO bzk.locatie (naam, datum_begin, ind_groep_jn, regeling_id, geometrietype, identificatie) " +
                "VALUES (?, ?, true, ?, ?, ?) " +
                "RETURNING id";
        KeyHolder keyHolder = new GeneratedKeyHolder();

        String objectType = "vlak".equals(geometryType) ? "gebied" : geometryType;
        String identificatie = "nl.imow-" + bgCode + "." + objectType + "engroep." + UUID.randomUUID().toString().replace("-", "").toLowerCase();
        jdbcTemplate.update(
                conn -> {
                    PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, name);
                    ps.setDate(2, Date.valueOf(dateStart));
                    ps.setInt(3, regelingId);
                    ps.setString(4, geometryType);
                    ps.setString(5, identificatie);

                    return ps;
                },
                keyHolder
        );

        return Optional.ofNullable(keyHolder.getKey())
            .map(Number::intValue)
            .orElseThrow(() -> new IllegalStateException("Er ging iets mis bij het inserten van de locatie"));
    }

    private void linkLocatieToGroep(int locatieId, int locatieGroepId) {
        String sql = "INSERT INTO bzk.groep_locatie (locatiegroep_id, locatie_id) VALUES (?, ?)";

        jdbcTemplate.update(sql, ps -> {
            ps.setInt(1, locatieGroepId);
            ps.setInt(2, locatieId);
        });
    }

    private void insertInformatieObjectVersie(String frbrWork, String frbrExpression, int regelingId, int eindverantwoordelijkeId,
                      int makerId, String gioName, String achtergrondVerwijzing, String achergrondActualiteit, String nauwkeurigheid,
                      int locatieId) {
        String sql =
                "INSERT INTO bzk.informatieobjectversie (frbr_work, frbr_expression, soort_work_id, regeling_id, eindverantwoordelijke_id, maker_id, " +
                        "formaat_informatieobject_id, naam, officiele_titel, publicatie_instructie_id, stop_schema_versie, achtergrond_verwijzing, " +
                        "achtergrond_actualiteit, nauwkeurigheid, locatie_id) " +
                "VALUES (?, ?, 2056, ?, ?, ?, 1410, ?, ?, 3, '1.3.0', ?, ?, ?, ?)";

        jdbcTemplate.update(sql, ps -> {
            ps.setObject(1, frbrWork);
            ps.setObject(2, frbrExpression);
            ps.setObject(3, regelingId);
            ps.setObject(4, eindverantwoordelijkeId);
            ps.setObject(5, makerId);
            ps.setObject(6, gioName);
            ps.setObject(7, frbrWork);
            ps.setObject(8, achtergrondVerwijzing);
            ps.setObject(9, achergrondActualiteit != null ? Date.valueOf(achergrondActualiteit) : null);
            ps.setObject(10, nauwkeurigheid != null ? Integer.parseInt(nauwkeurigheid) : null);
            ps.setObject(11, locatieId);
        });
    }

    private static class XPathBuilder {
        private final Map<String, String> namespaces = new HashMap<>();

        public XPathBuilder xmlns(String prefix, String namespaceURI) {
            namespaces.put(prefix, namespaceURI);
            return this;
        }

        public XPath build() {
            XPath xp = XPathFactory.newInstance().newXPath();
            xp.setNamespaceContext(new NamespaceContext() {

                @Override
                public String getNamespaceURI(String prefix) {
                    return namespaces.get(prefix);
                }

                @Override
                public String getPrefix(String namespaceURI) {
                    return getPrefixes(namespaceURI).next();
                }

                @Override
                public Iterator<String> getPrefixes(String namespaceURI) {
                    return namespaces.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(namespaceURI))
                        .map(Map.Entry::getKey)
                        .iterator();
                }
            });

            return xp;
        }
    }
}
