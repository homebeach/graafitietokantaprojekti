package XMLToCypher;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class MyListener extends xpathBaseListener {

    private String query;
    private Object lastElement;
    private Object conditionAppliesTo;
    private Object returnAttribute;
    private Object lastAttribute;
    private boolean attribute;
    private boolean insidePredicate;
    private Object predicateValue;
    
    private String functionName;
    private boolean insideFunction;
    
    private boolean alreadyCompared = false;

    public StringBuilder cypherQuery = new StringBuilder();
    
    public StringBuilder wholeQuery = new StringBuilder();
    private ArrayList wholeQueryParts = new ArrayList();
    
    private ArrayList predicatePaths = new ArrayList();
    private ArrayList matchQueries = new ArrayList();

    private ArrayList andSteps = new ArrayList(); 
    private StringBuilder andQuery = new StringBuilder();
    private ArrayList andParts = new ArrayList();
    
    private ArrayList matchParts = new ArrayList();
    private ArrayList predicateParts = new ArrayList();
    private ArrayList whereParts = new ArrayList();
    
    private StringBuilder matchPart = new StringBuilder();
    private StringBuilder wherePart = new StringBuilder();
    
    private Stack<ArrayList> paths = new Stack<ArrayList>();
    private Stack<String> predicateElements = new Stack<String>(); 

    private ArrayList orSteps = new ArrayList();
    private StringBuilder orQuery = new StringBuilder();
    private StringBuilder whereQuery = new StringBuilder();

    private int aliasIndex = 0;
    private int randomIndex = 0;
    private String lastRandom = "";

    public MyListener() {
        this.query = "";
    }

    public void setQuery(Object s) {
        this.query = this.query + s;
    }

    public Object getQuery() {
        return query;
    }

    @Override
    public void exitMain(xpathParser.MainContext ctx) {
        
    }
    
    @Override
    public void exitUnaryExprNoRoot(xpathParser.UnaryExprNoRootContext ctx) {
        if (!this.insidePredicate) {
            for (int i = 0; i < this.wholeQueryParts.size(); i++) {
                if (i > 0) {
                    this.wholeQuery.append(" UNION ");
                }
                this.wholeQuery.append(this.wholeQueryParts.get(i));
            }
            
            this.wholeQueryParts.clear();
        }
    }
    
    @Override
    public void exitUnionExprNoRoot(xpathParser.UnionExprNoRootContext ctx) {
        
        
    }
    
    @Override
    public void exitLocationPath(xpathParser.LocationPathContext ctx) {
        
        //If we are no longer inside predicate, it means that we are exiting from the whole query
        
        if (!this.insidePredicate) {
            cypherQuery.append("MATCH ");
            for (int i = this.matchQueries.size() - 1; i >= 0; i--) {
                if (i < this.matchQueries.size() - 1) {
                    cypherQuery.append(", ");
                }
                cypherQuery.append(this.matchQueries.get(i));
            }

            if (wherePart.length() > 0) {
                cypherQuery.append(" WHERE " + wherePart);
            }
            StringBuilder sb = new StringBuilder();
            sb.append(this.lastElement);

            if (sb.toString().equals("*")) {
                if (this.insideFunction) {
                    cypherQuery.append(" RETURN " + this.functionName + "(" + this.lastRandom + ")");
                } else {
                    
                    cypherQuery.append(" RETURN (" + this.lastRandom + ")");
                }
                
            }  else {
                if (this.returnAttribute != null) {
                    if (this.insideFunction) {
                        cypherQuery.append(" RETURN " + this.functionName + "(" + this.lastElement + "." + this.returnAttribute + ")");
                    } else {
                        cypherQuery.append(" RETURN (" + this.lastElement + "." + this.returnAttribute + ")");
                    }
                    
                } else {
                    if (this.insideFunction) {
                        cypherQuery.append(" RETURN " + this.functionName + "(" + this.lastElement + ")");
                    } else {
                        cypherQuery.append(" RETURN (" + this.lastElement + ")");
                    }
                    
                }
            }
            
            this.wholeQueryParts.add(this.cypherQuery);
            this.cypherQuery = new StringBuilder();
            this.lastElement = new Object();
            this.returnAttribute = null;
            this.lastRandom = "";
            this.wherePart = new StringBuilder();
            this.matchPart = new StringBuilder();
            this.aliasIndex = 0;
            this.randomIndex = 0;
        }
        
    }

    @Override
    public void enterAbsoluteLocationPathNoroot(xpathParser.AbsoluteLocationPathNorootContext ctx) {
    }
    
    @Override
    public void enterFunctionCall(xpathParser.FunctionCallContext ctx) {
        this.insideFunction = true;
    }
    
    @Override
    public void exitFunctionCall(xpathParser.FunctionCallContext ctx) {
        this.insideFunction = false;
    }
    
    @Override
    public void exitFunctionName(xpathParser.FunctionNameContext ctx) {
        if (ctx.getChildCount() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append(ctx.getChild(0));
            this.functionName = sb.toString();
        }
    }

    @Override
    public void exitAbsoluteLocationPathNoroot(xpathParser.AbsoluteLocationPathNorootContext ctx) {
        if (this.insidePredicate) {
            
            this.predicateParts.add("//");
        }
    }

    @Override
    public void exitNCName(xpathParser.NCNameContext ctx) {

        if (this.insidePredicate) {
            if (this.attribute) {
                this.lastAttribute = ctx.getChild(0);
            } else {
                String s = "alias" + this.aliasIndex + ": " + ctx.getChild(0);
                System.out.println("added: " + s);
                this.predicateParts.add(s);
                this.lastElement = "alias" + this.aliasIndex;
            }
        } else {
            //Tarkastetaanko onko kyseinen elementti tarkoitettu attribuutiksi.
            if (this.attribute) {
                
                //Jos on attribuutti, se asetetaan muuttujaan, joka myöhemmin palautetaan RETURN-yhteydessä.
                this.returnAttribute = ctx.getChild(0);
                this.attribute = false;
            } else {
                this.matchParts.add("alias" + this.aliasIndex + ": " + ctx.getChild(0));
                
                //Siirretty if-elsen ulkopuolelta
                this.lastElement = "alias" + this.aliasIndex;
            }
        }
        
        this.aliasIndex++;
        System.out.println("predikaatin osat lisäyksen jälkeen:");
        System.out.println(this.predicateParts);

        
    }
    
    @Override
    public void enterRelativeLocationPath(xpathParser.RelativeLocationPathContext ctx) {
        
        
    }

    @Override
    public void exitRelativeLocationPath(xpathParser.RelativeLocationPathContext ctx) {
        
        //Koska Cypherin polku alkaa aina solmusta, asetetaan isNode-lippumuuttuja todeksi.
        boolean thisIsNode = true;
        boolean lastWasWildCard = false;
        System.out.println("predicates to be processed");
        System.out.println(this.predicateParts);
        
        //Jos attribuutti-lippu on true, 
        if (this.attribute) {
            //andQuery.append(this.conditionAppliesTo + "." + this.returnAttribute);
        } else {
            //Jos polku on predikaatin sisällä liitetään polku MATCH-osaan.
            //TODO:
            //Liitetään polku MATCH osaan pilkulla erotettuna!
            if (this.insidePredicate) {
                
                //Aloitetaan WHERE-osa antamalla ensimmäiseksi solmuksi predikaattiin sidottu elementti
                this.matchPart.append("(" + this.predicateElements.peek() + ")");
                thisIsNode = false;
                
                //Käydään läpi predikaatin osat
                for (int i = 0; i < this.predicateParts.size(); i++) {
                    
                    //Jos kyseessä on ensimmäinen askel...
                    if (i == 0) {
                        
                        //Jos predikaatin aloittaa villi kortti, lisätään nuoli
                        if (this.predicateParts.get(i).toString().equals("*")) {
                            this.matchPart.append("-->");
                            if (i == this.predicateParts.size() - 1) {
                                this.matchPart.append("()");
                            }
                            
                            lastWasWildCard = true;
                            thisIsNode = !thisIsNode;
                        
                        //Jos taas takaisinmenoaskel, lisätään nuoli toiseen suuntaan
                        } else if (this.predicateParts.get(i).toString().equals("..")) {
                            this.matchPart.append("<--");
                            if (i == this.predicateParts.size() - 1) {
                                this.matchPart.append("()");
                            }
                            lastWasWildCard = true;
                            thisIsNode = !thisIsNode;
                            
                        //Jos elementtiin on osunut kaksoiskauttaviivat
                        } else if (this.predicateParts.get(i).toString().equals("//")) {
                            this.matchPart.append("-[*]->");
                            thisIsNode = true;
                        } else {
                            //Muussa tapauksessa oletetaan elementin olevan kaari.
                            this.matchPart.append("-");
                            this.matchPart.append("[" + this.predicateParts.get(i) + "]");
                            if (i == this.predicateParts.size() - 1) {
                                this.matchPart.append("->()");
                            }
                            thisIsNode = true;
                                
                            //(//) = -[*]->
                            lastWasWildCard = false;
                        }
                        
                        thisIsNode = true;
                        
                    //Muussa tapauksessa, kun i > 0
                    } else {
                        
                        //Tarkista edeltävän askelen tyyppi (/ vai //)
                        StringBuilder sb = new StringBuilder();
                        sb.append(ctx.getChild(i*2-1));
                        
                        //TODO
                        //Tarkista onko askel elementti vai villi kortti
                        if (this.predicateParts.get(i).toString().equals("*")) {
                            if (!thisIsNode) {
                                this.matchPart.append("-->");
                            } else {
                                this.matchPart.append("()");
                            }
                            
                            lastWasWildCard = true;
                            thisIsNode = !thisIsNode;
                            
                        } else if (this.predicateParts.get(i).toString().equals("..")) {
                            if (!thisIsNode) {
                                this.matchPart.append("<--");
                            } else {
                                this.matchPart.append("()");
                            }
                            lastWasWildCard = true;
                            thisIsNode = !thisIsNode;
                            
                        //Muussa tapauksessa edetään "normaalisti"
                        } else {
                            
                            //(/) = Joko - tai -> riippuen onko kaari tai solmu
                            if (sb.toString().equals("/")) {
                                if (lastWasWildCard) {
                                    this.matchPart.append("(" + this.predicateParts.get(i) + ")");
                                    lastWasWildCard = !lastWasWildCard;
                                    thisIsNode = false;
                                } else {
                                    if (thisIsNode) {
                                       this.matchPart.append("->");
                                       this.matchPart.append("(" + this.predicateParts.get(i) + ")");
                                       thisIsNode = false;
                                    } else {
                                       this.matchPart.append("-");
                                       this.matchPart.append("[" + this.predicateParts.get(i) + "]");
                                       thisIsNode = true;
                                    }
                                }
                                
                            //(//) = -[*]->
                            } else if (sb.toString().equals("//")) {
                                this.matchPart.append("-[*]->");
                                this.matchPart.append("(" + this.predicateParts.get(i) + ")");
                                thisIsNode = true;
                            }
                            lastWasWildCard = false;
                        }
                        
                        
                        
                        
                    }
                    
                }
                this.matchQueries.add(this.matchPart);
                this.predicateParts = new ArrayList();
                this.matchPart = new StringBuilder();
                
            //Jos ei, muodostetaan MATCH-kysely
            } else {
                for (int i = 0; i < this.matchParts.size(); i++) {
                    
                    //Jos kyseessä on ensimmäinen askel, on oletuksen mukaan sen oltava solmu (tai attribuutti?)
                    if (i == 0) {
                        
                        //Ensimmäinen askel voi olla villi kortti
                        if (this.matchParts.get(0).equals("*")) {
                            this.matchPart.append("(random" + this.randomIndex + ")");
                            this.lastRandom = "random" + this.randomIndex;
                            this.randomIndex++;
                            lastWasWildCard = true;
                            
                        //Tai ihan normi solmu
                        } else {
                            this.matchPart.append("(" + this.matchParts.get(0) + ")");
                        }
                        thisIsNode = false;
                        
                    //Muussa tapauksessa..
                    } else {
                        
                        //Tarkista edeltävän askelen tyyppi (/ vai //)
                        StringBuilder sb = new StringBuilder();
                        sb.append(ctx.getChild(i*2-1));
                        
                        //TODO
                        //Tarkista onko askel elementti vai villi kortti
                        if (this.matchParts.get(i).toString().equals("*")) {
                            if (!lastWasWildCard) {
                                if (!thisIsNode) {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("-[random" + this.randomIndex + "]");
                                        this.matchPart.append("->()");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                    } else {
                                        this.matchPart.append("-->");
                                        thisIsNode = true;
                                    }
                                    
                                } else {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("->(random" + this.randomIndex + ")");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                        thisIsNode = false;
                                        
                                    } else {
                                        this.matchPart.append("()");
                                        thisIsNode = false;
                                    }
                                    
                                }
                            } else {
                                if (!thisIsNode) {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("-[random" + this.randomIndex + "]");
                                        this.matchPart.append("->()");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                        thisIsNode = false;
                                        
                                    } else {
                                        this.matchPart.append("-->");
                                        thisIsNode = true;
                                    }
                                    
                                } else {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("(random" + this.randomIndex + ")");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                        thisIsNode = false;
                                        
                                    } else {
                                        this.matchPart.append("()");
                                        thisIsNode = false;
                                    }
                                    
                                }
                            }
                            
                            
                            lastWasWildCard = true;
                            
                        } else if (this.matchParts.get(i).toString().equals("..")) {
                            if (!lastWasWildCard) {
                                if (!thisIsNode) {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("<-[random" + this.randomIndex + "]");
                                        this.matchPart.append("-()");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                    } else {
                                        this.matchPart.append("<--");
                                        thisIsNode = true;
                                    }
                                    
                                } else {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("<-(random" + this.randomIndex + ")");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                        thisIsNode = false;
                                        
                                    } else {
                                        this.matchPart.append("()");
                                        thisIsNode = false;
                                    }
                                    
                                }
                            } else {
                                if (!thisIsNode) {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("<-[random" + this.randomIndex + "]");
                                        this.matchPart.append("-()");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                        thisIsNode = false;
                                        
                                    } else {
                                        this.matchPart.append("<--");
                                        thisIsNode = true;
                                    }
                                    
                                } else {
                                    if (i == this.matchParts.size() - 1) {
                                        this.matchPart.append("(random" + this.randomIndex + ")");
                                        this.lastRandom = "random" + this.randomIndex;
                                        this.randomIndex++;
                                        thisIsNode = false;
                                        
                                    } else {
                                        this.matchPart.append("()");
                                        thisIsNode = false;
                                    }
                                    
                                }
                            }
                            
                            
                            lastWasWildCard = true;
                            
                        //Muussa tapauksessa edetään "normaalisti"
                        } else {
                            
                            //(/) = Joko - tai -> riippuen onko kaari tai solmu
                            if (sb.toString().equals("/")) {
                                if (lastWasWildCard) {
                                    if (thisIsNode) {
                                        this.matchPart.append("(" + this.matchParts.get(i) + ")");
                                        thisIsNode = false;
                                    } else {
                                        this.matchPart.append("-[" + this.matchParts.get(i) + "]");
                                        if (i == this.matchParts.size() - 1) {
                                            this.matchPart.append("->()");
                                        }
                                        thisIsNode = true;
                                    }
                                    
                                    lastWasWildCard = !lastWasWildCard;
                                    
                                } else {
                                    if (thisIsNode) {
                                       this.matchPart.append("->");
                                       this.matchPart.append("(" + this.matchParts.get(i) + ")");
                                       thisIsNode = false;
                                    } else {
                                       this.matchPart.append("-");
                                       this.matchPart.append("[" + this.matchParts.get(i) + "]");
                                       thisIsNode = true;
                                    }
                                }
                                
                            //(//) = -[*]->
                            } else if (sb.toString().equals("//")) {
                                this.matchPart.append("-[*]->");
                                this.matchPart.append("(" + this.matchParts.get(i) + ")");
                                thisIsNode = true;
                            }
                            lastWasWildCard = false;
                        }
                        
                        
                        
                        
                    }
                }
                this.matchQueries.add(this.matchPart);
            }
        }
    }

    @Override
    public void enterStep(xpathParser.StepContext ctx) {
    }

    @Override
    public void exitStep(xpathParser.StepContext ctx) {
        
    }

    @Override
    public void exitPrimaryExpr(xpathParser.PrimaryExprContext ctx) {
        this.predicateValue = ctx.getChild(0);
    }

    @Override
    public void exitAxisSpecifier(xpathParser.AxisSpecifierContext ctx) {

        //Jos lapsi ei ole null, on havaittu @-merkki, jolloin elementti on attribuutti.
        if (ctx.getChild(0) != null) {
            this.attribute = true;
            
        }
    }

    @Override
    public void exitEqualityExpr(xpathParser.EqualityExprContext ctx) {
        //Jos kyseessä on attribuutti, lisätään operaattori ja attribuutin arvo.
        if (this.attribute && this.insidePredicate && !this.alreadyCompared) {
            StringBuilder eq = new StringBuilder();
            eq.append(ctx.getChild(1));
            String equalOrInEqual = eq.toString();
            if (equalOrInEqual.equals("=")) {
                this.andSteps.add(this.conditionAppliesTo + "." + this.lastAttribute + " = " + this.predicateValue);
            } else if (equalOrInEqual.equals("!=")) {
                this.andSteps.add(this.conditionAppliesTo + "." + this.lastAttribute + " <> " + this.predicateValue);
            } else {
                this.andSteps.add("EXISTS(" + this.conditionAppliesTo + "." + this.lastAttribute + ")");
            }
        } else {
            
        }
    }

    @Override
    public void exitRelationalExpr(xpathParser.RelationalExprContext ctx) {
        //Jos kyseessä on attribuutti, lisätään operaattori ja attribuutin arvo.
        if (this.attribute && this.insidePredicate) {
            StringBuilder eq = new StringBuilder();
            eq.append(ctx.getChild(1));
            String equalOrInEqual = eq.toString();
            if (equalOrInEqual.equals(">")) {
                this.andSteps.add(this.conditionAppliesTo + "." + this.lastAttribute + " > " + this.predicateValue);
                this.alreadyCompared = true;
            } else if (equalOrInEqual.equals("<")) {
                this.andSteps.add(this.conditionAppliesTo + "." + this.lastAttribute + " < " + this.predicateValue);
                this.alreadyCompared = true;
            }
        } else {
            
        }
    }

    @Override
    public void enterAndExpr(xpathParser.AndExprContext ctx) {
        //And-operaattorila on aina pariton määrä lapsia, joista parittomat luvut ovat "and"-operaattoreita
        andSteps = new ArrayList();
    }


    @Override
    public void exitAndExpr(xpathParser.AndExprContext ctx) {
        //And-operaattorila on aina pariton määrä lapsia, joista parittomat luvut ovat "and"-operaattoreita
        if (this.andSteps.size() > 1) {
            for (int i = 0; i < this.andSteps.size(); i++) {
                if (i > 0) {
                    andQuery.append(" AND ");
                }
                andQuery.append(this.andSteps.get(i));

            }
        } else if (this.andSteps.size() == 1) {
            andQuery.append(this.andSteps.get(0));
        }
        this.andSteps.clear();
        andParts.add(andQuery);
        andQuery = new StringBuilder();
    }

    @Override
    public void enterOrExpr(xpathParser.OrExprContext ctx) {
        //And-operaattorila on aina pariton määrä lapsia, joista parittomat luvut ovat "and"-operaattoreita
        orSteps = new ArrayList();
    }


    @Override
    public void exitOrExpr(xpathParser.OrExprContext ctx) {
        //And-operaattorila on aina pariton määrä lapsia, joista parittomat luvut ovat "and"-operaattoreita
        if (this.andParts.size() > 1) {
            for (int i = 0; i < this.andParts.size(); i++) {
                if (i > 0) {
                    wherePart.append(" OR ");
                }
                wherePart.append(this.andParts.get(i));

            }
        } else if (this.andParts.size() == 1) {
            wherePart.append(this.andParts.get(0));
        }
        this.orSteps.clear();
        this.andParts.clear();
    }


    @Override
    public void enterNodeTest(xpathParser.NodeTestContext ctx) {
    }

    @Override
    public void exitExpr(xpathParser.ExprContext ctx) {
        if (this.attribute && this.insidePredicate) {
            this.attribute = false;
        }
    }


    @Override
    public void exitNameTest(xpathParser.NameTestContext ctx) {
        System.out.println("hhhh");
        
        StringBuilder g = new StringBuilder();
        g.append(ctx.getChild(0));
        if (g.toString().equals("*")) {
            System.out.println("hhhh");
            this.lastElement = "random" + this.randomIndex;
            if (this.insidePredicate) {
                this.predicateParts.add(g.toString());
                System.out.println("visited!");
            } else {
                this.matchParts.add(g.toString());
            }

        }
        
    }

    @Override
    public void enterPredicate(xpathParser.PredicateContext ctx) {
        this.predicateElements.push(this.lastElement.toString());
        this.conditionAppliesTo = this.lastElement;
        this.insidePredicate = true;
        if (this.wherePart.length() > 0) {
            wherePart.append(" AND ");
        }
      //Jos paths on empty, oleteteaan että ollaan ylimmällä tasolla, eli predikaattien ulkopuolella
        if (this.paths.empty()) {
            this.paths.push(this.matchParts);
        } else {
            this.paths.push(this.predicateParts); 
            this.predicateParts = new ArrayList();
        }
            
    }

    @Override
    public void exitPredicate(xpathParser.PredicateContext ctx) {
        String h = this.predicateElements.pop();
        
        if (this.predicateElements.empty()) {
            this.insidePredicate = false;
        }
        if (this.insidePredicate) {
            this.predicateParts = this.paths.pop();
        } else {
            this.predicateParts = new ArrayList();
        }
        
        this.lastElement = h;
        this.alreadyCompared = false;
    }

    @Override
    public void exitAbbreviatedStep(xpathParser.AbbreviatedStepContext ctx) {
        System.out.println("here!!");
        StringBuilder sb = new StringBuilder();
        sb.append(ctx.getChild(0));
        System.out.println("mmm" + sb.toString());

        if (this.insidePredicate) {
            this.predicateParts.add("..");
        } else {
            this.matchParts.add("..");
        }
        
        this.lastElement = "random" + this.randomIndex;
    }
}