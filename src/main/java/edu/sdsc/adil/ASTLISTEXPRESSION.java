/* Generated By:JJTree: Do not edit this line. ASTLISTEXPRESSION.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTLISTEXPRESSION extends SimpleNode {
  public ASTLISTEXPRESSION(int id) {
    super(id);
  }

  public ASTLISTEXPRESSION(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=e68e5a843e0aadfa57cc08f5ef5c4199 (do not edit this line) */
