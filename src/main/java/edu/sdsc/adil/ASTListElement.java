/* Generated By:JJTree: Do not edit this line. ASTListElement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTListElement extends SimpleNode {
  public ASTListElement(int id) {
    super(id);
  }

  public ASTListElement(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=eb39097ca0fa377a78ff95eb96bfeaee (do not edit this line) */
