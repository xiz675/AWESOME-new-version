/* Generated By:JJTree: Do not edit this line. ASTConstructGraphFromRelationExpression.java Version 7.0 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=false,NODE_PREFIX=AST,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package edu.sdsc.adil;

public
class ASTConstructGraphFromRelationExpression extends SimpleNode {
  public ASTConstructGraphFromRelationExpression(int id) {
    super(id);
  }

  public ASTConstructGraphFromRelationExpression(Adil p, int id) {
    super(p, id);
  }


  /** Accept the visitor. **/
  public Object jjtAccept(AdilVisitor visitor, Object data) {

    return
    visitor.visit(this, data);
  }
}
/* JavaCC - OriginalChecksum=57743cc08601cbf306ea874f08e93a02 (do not edit this line) */
