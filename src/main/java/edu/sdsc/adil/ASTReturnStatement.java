/* Generated By:JJTree: Do not edit this line. ASTReturnStatement.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTReturnStatement extends SimpleNode {
  public ASTReturnStatement(int id) {
    super(id);
  }

  public ASTReturnStatement(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=8c0e50f6292f16c809b49eb5b06df1b5 (do not edit this line) */
