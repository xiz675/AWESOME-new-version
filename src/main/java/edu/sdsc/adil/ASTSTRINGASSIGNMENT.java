/* Generated By:JJTree: Do not edit this line. ASTSTRINGASSIGNMENT.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTSTRINGASSIGNMENT extends SimpleNode {
  public ASTSTRINGASSIGNMENT(int id) {
    super(id);
  }

  public ASTSTRINGASSIGNMENT(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=0ff86ec414e4afdaed0cb1abd5b8b70a (do not edit this line) */
