/* Generated By:JJTree: Do not edit this line. ASTCreateViewStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTCreateViewStatement extends SimpleNode {
  public ASTCreateViewStatement(int id) {
    super(id);
  }

  public ASTCreateViewStatement(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=745b7697dec7039fb31f4da43b27054a (do not edit this line) */